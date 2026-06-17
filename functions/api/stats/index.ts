// GET /api/stats?token=...&days=30&os=all&version=all
//
// 全部指标直接从 pings 表派生(而非 daily_summary 缓存),这样:
//   1) 支持 os / version 筛选,把整张看板切到任意子群体;
//   2) 窗口锚定到"数据里的最新日期",而不是服务器 UTC,消除跨时区偏移;
//   3) 新增 returning_users(DAU = 新增 + 回访)、rolling retention(全量活跃留存)、
//      累计增长曲线等更专业的指标。
// 保留所有旧字段,旧版看板照常工作;新版看板用新增字段。
export const onRequest = async (context) => {
  const url = new URL(context.request.url);
  const DB = context.env.DB;
  const daysRaw = url.searchParams.get("days") ?? "30";     // "all" 表示全部历史
  const daysParam = daysRaw === "all" ? 99999 : Math.min(parseInt(daysRaw, 10), 365);
  const startParam = url.searchParams.get("start");   // 可选 YYYY-MM-DD(自定义起点)
  const endParam = url.searchParams.get("end");       // 可选 YYYY-MM-DD(自定义终点)
  const osFilter = (url.searchParams.get("os") ?? "all").toLowerCase(); // all|win|linux|mac
  const verFilter = url.searchParams.get("version") ?? "all";
  // 用户群筛选(设备级 cohort):
  //   all       全部设备
  //   new       本期新增:首次出现日落在所选区间 [startDate, today] 的设备
  //   returning 存量回访:区间开始前就已存在、区间内仍活跃的设备
  const segment = (url.searchParams.get("segment") ?? "all").toLowerCase();

  // 公共筛选片段:所有聚合共用,保证整页看板一致地切到同一子群体。
  const filters = [];
  if (osFilter !== "all") filters.push({ sql: "os = ?", val: osFilter });
  if (verFilter !== "all") filters.push({ sql: "version = ?", val: verFilter });
  const filterSql = filters.map((f) => f.sql).join(" AND ");
  const filterBinds = filters.map((f) => f.val);
  const filterAnd = filterSql ? " AND " + filterSql : "";      // 用于已有 WHERE 的拼接
  const filterWhere = filterSql ? " WHERE " + filterSql : "";  // 用于尚无 WHERE 的整段查询

  try {
    // 数据域内的最新日期——用数据本身而非服务器 UTC,避免 WAU/MAU 窗口被时区推移。
    const latestRow = await DB.prepare(
      "SELECT MAX(date) AS d FROM pings" + filterWhere
    ).bind(...filterBinds).first();
    const today = endParam && /^\d{4}-\d{2}-\d{2}$/.test(endParam) ? endParam : (latestRow?.d ?? new Date().toISOString().slice(0, 10));
    const days = startParam && /^\d{4}-\d{2}-\d{2}$/.test(startParam)
      ? Math.max(1, Math.round((new Date(today) - new Date(startParam)) / 86400000) + 1)
      : daysParam;

    // ── 起点:自定义 > 全部历史(用数据最早日) > 回看 N 天 ──
    let startDate;
    if (startParam && /^\d{4}-\d{2}-\d{2}$/.test(startParam)) {
      startDate = startParam;
    } else if (daysRaw === "all") {
      const minRow = await DB.prepare("SELECT MIN(date) AS d FROM pings" + filterWhere).bind(...filterBinds).first();
      startDate = minRow?.d ?? today;
    } else {
      startDate = addDays(today, -(days - 1));
    }

    // ── 用户群 cohort 片段:基于设备首次出现日划定的子集 ──
    // new/returning 都是相对"所选区间起点 startDate"定义的;固定一次,套到所有适用聚合上。
    let cohortAnd = "";        // 拼在 filterAnd 之后
    let cohortBinds = [];
    if (segment === "new") {
      cohortAnd = " AND device_id IN (SELECT device_id FROM pings WHERE first_seen = 1 AND date BETWEEN ? AND ?)";
      cohortBinds = [startDate, today];
    } else if (segment === "returning") {
      cohortAnd = " AND device_id IN (SELECT device_id FROM pings WHERE first_seen = 1 AND date < ?)";
      cohortBinds = [startDate];
    }
    // 通用尾部:filterAnd + cohortAnd 一起出现,binds 也 filterBinds + cohortBinds 顺序一致。
    const allBinds = [...filterBinds, ...cohortBinds];
    const allAnd = filterAnd + cohortAnd;
    // ── 日趋势:直接 GROUP BY date(支持筛选 + returning_users + 用户群 cohort)──
    const trendsQ = await DB.prepare(
      "SELECT date, " +
        "COUNT(*) AS dau, " +
        "SUM(CASE WHEN msg_count > 0 THEN 1 ELSE 0 END) AS eff_dau, " +
        "SUM(CASE WHEN first_seen THEN 1 ELSE 0 END) AS new_users, " +
        "SUM(CASE WHEN first_seen = 0 THEN 1 ELSE 0 END) AS returning_users, " +
        "SUM(msg_count) AS total_msgs, " +
        "SUM(CASE WHEN os = 'win'   THEN 1 ELSE 0 END) AS win_users, " +
        "SUM(CASE WHEN os = 'linux' THEN 1 ELSE 0 END) AS linux_users, " +
        "SUM(CASE WHEN os = 'mac'   THEN 1 ELSE 0 END) AS mac_users " +
        "FROM pings WHERE date BETWEEN ? AND ? " + allAnd + " " +
        "GROUP BY date ORDER BY date"
    ).bind(startDate, today, ...allBinds).all();
    const trends = trendsQ.results ?? [];

    const todayRow = trends[trends.length - 1] ?? {};
    const yesterday = trends[trends.length - 2] ?? {};
    const dau = todayRow?.dau ?? 0;

    // ── WAU / MAU(窗口锚定到数据最新日期;cohort 限定到所选用户群)──
    const wau = await uniqueDevices(DB, today, 7, allAnd, allBinds);
    const mau = await uniqueDevices(DB, today, 30, allAnd, allBinds);
    const stickinessMau = mau > 0 ? Math.round((dau / mau) * 100) : 0;
    const stickinessWau = wau > 0 ? Math.round((dau / wau) * 100) : 0;

    // ── 累计用户(筛选 + 用户群 cohort 后的设备总数)──
    const totalRow = await DB.prepare(
      "SELECT COUNT(DISTINCT device_id) AS cnt FROM pings WHERE 1=1 " + allAnd
    ).bind(...allBinds).first();
    const totalUsers = totalRow?.cnt ?? 0;

    // ── 历史峰值 DAU(SQLite 裸列随 MAX 取峰值所在行,得到 peakDate)──
    const peakRow = await DB.prepare(
      "SELECT MAX(dau) AS peak, date FROM (" +
        "SELECT date, COUNT(*) AS dau FROM pings WHERE 1=1 " + allAnd + " GROUP BY date" +
      ")"
    ).bind(...allBinds).first();
    const peakDau = peakRow?.peak ?? 0;
    const peakDate = peakRow?.date ?? null;

    // ── 当日有效用户平均消息数 ──
    const avgMsgs = await DB.prepare(
      "SELECT AVG(msg_count) AS avg FROM pings WHERE date = ? AND msg_count > 0 " + allAnd
    ).bind(today, ...allBinds).first();

    // ── 会话深度分布(过去 7 天)──
    const sevenDaysAgo = addDays(today, -6);
    const buckets = await DB.prepare(
      "SELECT " +
        "SUM(CASE WHEN msg_count = 0 THEN 1 ELSE 0 END) AS b0, " +
        "SUM(CASE WHEN msg_count BETWEEN 1 AND 5 THEN 1 ELSE 0 END) AS b1_5, " +
        "SUM(CASE WHEN msg_count BETWEEN 6 AND 20 THEN 1 ELSE 0 END) AS b6_20, " +
        "SUM(CASE WHEN msg_count > 20 THEN 1 ELSE 0 END) AS b20p " +
        "FROM pings WHERE date >= ? " + allAnd
    ).bind(sevenDaysAgo, ...allBinds).first();

    // ── 版本分布(最新日;版本筛选下只剩一项,无妨)──
    const versions = await DB.prepare(
      "SELECT version, COUNT(*) AS count FROM pings WHERE date = ? " + allAnd + " GROUP BY version ORDER BY count DESC"
    ).bind(today, ...allBinds).all();

    // ── 累计用户增长曲线(按首次出现日累计 distinct 设备;叠用户群 cohort,
    //    使"总用户增长"折线也响应全部筛选)──
    const growth = await computeGrowth(DB, allAnd, allBinds);

    // ── 留存:新用户 cohort + 全量滚动留存 ──
    const retention = await computeRetention(DB, today, 60, filterAnd, filterBinds);
    const rollingRetention = await computeRollingRetention(DB, today, 14, filterAnd, filterBinds);
    const avgRetention = computeAvgRetention(retention.cohorts);

    // ── 环比(本期 vs 等长上期):KPI 卡做"较上期 ±%"──
    // 仅在"全部用户群 + 非全期"时计算:cohort 视图下"上期"群体定义不一致,
    // 全期下没有等长"上期",两种情况下环比都没意义,直接置零(前端隐藏 delta)。
    let popRow = null;
    if (segment === "all" && daysRaw !== "all") {
      const prevStart = addDays(today, -(2 * days - 1));
      const prevEnd = addDays(today, -days);
      popRow = await DB.prepare(
        "SELECT " +
          "(SELECT COUNT(DISTINCT device_id) FROM pings WHERE date BETWEEN ? AND ? " + filterAnd + ") AS cur_users, " +
          "(SELECT COUNT(DISTINCT device_id) FROM pings WHERE date BETWEEN ? AND ? " + filterAnd + ") AS prev_users, " +
          "(SELECT SUM(CASE WHEN first_seen THEN 1 ELSE 0 END) FROM pings WHERE date BETWEEN ? AND ? " + filterAnd + ") AS prev_new, " +
          "(SELECT SUM(msg_count) FROM pings WHERE date BETWEEN ? AND ? " + filterAnd + ") AS prev_msgs, " +
          "(SELECT COUNT(*) FROM pings WHERE date BETWEEN ? AND ? " + filterAnd + ") AS prev_user_days"
      ).bind(
        startDate, today, ...filterBinds,
        prevStart, prevEnd, ...filterBinds,
        prevStart, prevEnd, ...filterBinds,
        prevStart, prevEnd, ...filterBinds,
        prevStart, prevEnd, ...filterBinds
      ).first();
    }

    // ── 流失 / 复活(最近 7 天 vs 前 7 天;只认 os/version,不叠用户群 cohort)──
    const churn = await computeChurn(DB, today, filterAnd, filterBinds);

    // ── 参与度分位:中位数 / P90 / 均值(只看有消息的活跃设备,叠用户群 cohort)──
    // 均值会被重度用户拉高,中位数才是"典型用户"的真实强度。
    const percentiles = await computePercentiles(DB, startDate, today, allAnd, allBinds);

    // ── 版本采用曲线(近 30 天每日每版本 DAU;不受筛选影响,反映真实升级速度)──
    const versionTrend = await computeVersionTrend(DB, today);

    // ── 最近活跃设备列表(version/os 取该设备最新一条;叠用户群 cohort)──
    const recentUsers = await DB.prepare(
      "SELECT device_id, " +
        "MIN(date) AS first_date, MAX(date) AS last_date, " +
        "SUM(msg_count) AS total_msgs, COUNT(*) AS active_days, " +
        "(SELECT version FROM pings p2 WHERE p2.device_id = p.device_id ORDER BY date DESC LIMIT 1) AS version, " +
        "(SELECT os FROM pings p2 WHERE p2.device_id = p.device_id ORDER BY date DESC LIMIT 1) AS os " +
        "FROM pings p WHERE 1=1 " + allAnd + " " +
        "GROUP BY device_id ORDER BY last_date DESC, total_msgs DESC LIMIT 50"
    ).bind(...allBinds).all();

    return new Response(JSON.stringify({
      trends,
      wau, mau,
      stickiness: stickinessMau,         // 兼容老字段
      stickinessMau, stickinessWau,
      totalUsers,
      peakDau, peakDate,
      avgMsgsPerActive: Math.round((avgMsgs?.avg ?? 0) * 10) / 10,
      depth: {
        b0: buckets?.b0 ?? 0,
        b1_5: buckets?.b1_5 ?? 0,
        b6_20: buckets?.b6_20 ?? 0,
        b20p: buckets?.b20p ?? 0,
      },
      avgRetention,
      versions: versions.results ?? [],
      retention,
      rollingRetention,
      growth,
      recentUsers: recentUsers.results ?? [],
      pop: popRow ? {
        curUsers: popRow.cur_users ?? 0,
        prevUsers: popRow.prev_users ?? 0,
        prevNew: popRow.prev_new ?? 0,
        prevMsgs: popRow.prev_msgs ?? 0,
        prevUserDays: popRow.prev_user_days ?? 0,
      } : null,
      churn,
      percentiles,
      versionTrend,
      filter: { os: osFilter, version: verFilter, segment, range: daysRaw, startDate, asOf: today },
    }), {
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  } catch (err) {
    console.error("stats error:", err);
    return new Response(JSON.stringify({ error: String(err?.message ?? err) }), {
      status: 500,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  }
};

// 日期字符串加减天数(全程 UTC,避免本地时区污染 "YYYY-MM-DD")。
function addDays(dateStr, n) {
  const d = new Date(dateStr + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

async function uniqueDevices(DB, endDate, windowDays, filterAnd, filterBinds) {
  const startDate = addDays(endDate, -(windowDays - 1));
  const row = await DB.prepare(
    "SELECT COUNT(DISTINCT device_id) AS cnt FROM pings WHERE date BETWEEN ? AND ? " + filterAnd
  ).bind(startDate, endDate, ...filterBinds).first();
  return row?.cnt ?? 0;
}

// 全期累计用户增长:按每个 first_seen 日累计,得到一条单调上升曲线。
async function computeGrowth(DB, filterAnd, filterBinds) {
  const rows = await DB.prepare(
    "SELECT date, COUNT(*) AS new_on_day FROM pings WHERE first_seen = 1 " + filterAnd + " GROUP BY date ORDER BY date"
  ).bind(...filterBinds).all();
  const out = [];
  let cum = 0;
  for (const r of rows.results ?? []) {
    cum += r.new_on_day ?? 0;
    out.push({ date: r.date, total: cum, new: r.new_on_day ?? 0 });
  }
  return out;
}

// 新用户 cohort 留存:每个"首次出现日"的设备,在 D+N 日的回访率。
async function computeRetention(DB, asOf, cohortDays, filterAnd, filterBinds) {
  try {
    const since = addDays(asOf, -cohortDays);
    const cohorts = await DB.prepare(
      "SELECT date, COUNT(*) AS size FROM pings " +
        "WHERE first_seen = 1 AND date >= ? " + filterAnd + " " +
        "GROUP BY date ORDER BY date DESC LIMIT 30"
    ).bind(since, ...filterBinds).all();

    const result = [];
    const offsets = [1, 3, 7, 14, 30];
    for (const cohort of cohorts.results ?? []) {
      const retention = {};
      for (const offset of offsets) {
        const target = addDays(cohort.date, offset);
        if (target > asOf) continue;
        const row = await DB.prepare(
          "SELECT COUNT(*) AS cnt FROM pings " +
            "WHERE device_id IN (SELECT device_id FROM pings WHERE date = ? AND first_seen = 1 " + filterAnd + ") " +
            "AND date = ? " + filterAnd
        ).bind(cohort.date, ...filterBinds, target, ...filterBinds).first();
        retention[offset] = cohort.size > 0 ? Math.round(((row?.cnt ?? 0) / cohort.size) * 100) : 0;
      }
      result.push({ date: cohort.date, size: cohort.size, retention });
    }
    return { cohorts: result };
  } catch (err) {
    console.error("retention error:", err);
    return { cohorts: [] };
  }
}

// 全量滚动留存:以"某天所有活跃设备"为 cohort(不限新用户),看 D+N 回访率。
// 衡量产品对存量用户的整体粘性,与新用户 cohort 互补。
async function computeRollingRetention(DB, asOf, baseDays, filterAnd, filterBinds) {
  try {
    const since = addDays(asOf, -(baseDays - 1));
    const bases = await DB.prepare(
      "SELECT date, COUNT(*) AS size FROM pings WHERE date >= ? " + filterAnd + " GROUP BY date ORDER BY date DESC LIMIT ?"
    ).bind(since, ...filterBinds, baseDays).all();

    const result = [];
    const offsets = [1, 3, 7, 14];
    for (const base of bases.results ?? []) {
      const retention = {};
      for (const offset of offsets) {
        const target = addDays(base.date, offset);
        if (target > asOf) continue;
        const row = await DB.prepare(
          "SELECT COUNT(*) AS cnt FROM pings " +
            "WHERE device_id IN (SELECT device_id FROM pings WHERE date = ? " + filterAnd + ") " +
            "AND date = ? " + filterAnd
        ).bind(base.date, ...filterBinds, target, ...filterBinds).first();
        retention[offset] = base.size > 0 ? Math.round(((row?.cnt ?? 0) / base.size) * 100) : 0;
      }
      result.push({ date: base.date, size: base.size, retention });
    }
    return { cohorts: result };
  } catch (err) {
    console.error("rolling retention error:", err);
    return { cohorts: [] };
  }
}

// 聚合新用户 cohort 的 D1/D3/D7/D14/D30 加权平均(只取已"满龄"的 cohort,避免新日拉低均值)。
// cohort 本身已计算这些 offset,这里只是按 cohort 规模加权汇总,无额外查询。
function computeAvgRetention(cohorts) {
  const result = { d1: null, d3: null, d7: null, d14: null, d30: null };
  if (!cohorts?.length) return result;
  for (const [key, offset] of [["d1", 1], ["d3", 3], ["d7", 7], ["d14", 14], ["d30", 30]]) {
    const valid = cohorts.filter((c) => c.retention[offset] != null);
    if (!valid.length) continue;
    const totalUsers = valid.reduce((s, c) => s + c.size, 0);
    const weighted = valid.reduce((s, c) => s + c.retention[offset] * c.size, 0);
    result[key] = totalUsers > 0 ? Math.round(weighted / totalUsers) : null;
  }
  return result;
}

// 流失 / 复活分析:把"最近 7 天"与"前 7 天"对比。
//   retained  = 两窗都活跃的设备
//   churned   = 前窗活跃、最近没回来(流失)
//   resurrected = 最近活跃、前窗不在、但更早出现过(复活)
async function computeChurn(DB, asOf, filterAnd, filterBinds) {
  try {
    const rStart = addDays(asOf, -6), rEnd = asOf;            // 最近 7 天
    const pStart = addDays(asOf, -13), pEnd = addDays(asOf, -7); // 前 7 天
    const before = addDays(asOf, -14);                         // 早于前窗
    const recentQ = await DB.prepare(
      "SELECT COUNT(DISTINCT device_id) AS c FROM pings WHERE date BETWEEN ? AND ? " + filterAnd
    ).bind(rStart, rEnd, ...filterBinds).first();
    const priorQ = await DB.prepare(
      "SELECT COUNT(DISTINCT device_id) AS c FROM pings WHERE date BETWEEN ? AND ? " + filterAnd
    ).bind(pStart, pEnd, ...filterBinds).first();
    const retainedQ = await DB.prepare(
      "SELECT COUNT(DISTINCT device_id) AS c FROM pings WHERE date BETWEEN ? AND ? " + filterAnd +
      " AND device_id IN (SELECT device_id FROM pings WHERE date BETWEEN ? AND ? " + filterAnd + ")"
    ).bind(pStart, pEnd, ...filterBinds, rStart, rEnd, ...filterBinds).first();
    const resurrectedQ = await DB.prepare(
      "SELECT COUNT(DISTINCT device_id) AS c FROM pings WHERE date BETWEEN ? AND ? " + filterAnd +
      " AND device_id NOT IN (SELECT device_id FROM pings WHERE date BETWEEN ? AND ? " + filterAnd + ")" +
      " AND device_id IN (SELECT device_id FROM pings WHERE date < ? " + filterAnd + ")"
    ).bind(rStart, rEnd, ...filterBinds, pStart, pEnd, ...filterBinds, before, ...filterBinds).first();
    const recent = recentQ?.c ?? 0, prior = priorQ?.c ?? 0, retained = retainedQ?.c ?? 0, resurrected = resurrectedQ?.c ?? 0;
    return {
      recent, prior, retained, resurrected,
      churned: Math.max(0, prior - retained),
      churnRate: prior > 0 ? Math.round((prior - retained) / prior * 100) : null,
    };
  } catch (err) {
    console.error("churn error:", err);
    return null;
  }
}

// 活跃用户当日消息数的分位(排序后取中位 / P90)。均值被重度用户拉高,
// 中位数才能反映"典型用户"的真实参与强度。
async function computePercentiles(DB, start, end, filterAnd, filterBinds) {
  try {
    const rows = await DB.prepare(
      "SELECT msg_count FROM pings WHERE date BETWEEN ? AND ? AND msg_count > 0 " + filterAnd + " ORDER BY msg_count"
    ).bind(start, end, ...filterBinds).all();
    const arr = (rows.results ?? []).map((r) => r.msg_count);
    if (!arr.length) return { count: 0, median: 0, p90: 0, mean: 0 };
    const sum = arr.reduce((s, n) => s + n, 0);
    return {
      count: arr.length,
      median: arr[Math.floor(arr.length / 2)],
      p90: arr[Math.min(arr.length - 1, Math.floor(arr.length * 0.9))],
      mean: Math.round((sum / arr.length) * 10) / 10,
    };
  } catch (err) {
    console.error("percentiles error:", err);
    return null;
  }
}

// 版本采用曲线:近 30 天每日每版本 DAU。刻意不套筛选——这一图专门反映"新版本
// 滚动升级的速度",套了版本筛选就没意义了。
async function computeVersionTrend(DB, asOf) {
  try {
    const start = addDays(asOf, -29);
    const rows = await DB.prepare(
      "SELECT date, version, COUNT(*) AS c FROM pings WHERE date BETWEEN ? AND ? GROUP BY date, version ORDER BY date"
    ).bind(start, asOf).all();
    return rows.results ?? [];
  } catch (err) {
    console.error("versionTrend error:", err);
    return [];
  }
}
