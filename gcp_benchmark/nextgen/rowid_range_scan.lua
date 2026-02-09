-- RowID multi-range coprocessor stress for TiDB/CSE
-- Target: generate many disjoint _tidb_rowid ranges per query, crossing many regions.

local drv
local con

sysbench.cmdline.options = {
  table_name = {"Table name", "sbtest_rowid_scan"},
  table_size = {"Rows to load", 5000000},
  batch_size = {"Rows per INSERT batch", 1000},
  pad_size = {"Bytes for padding column", 64},
  split_regions = {"Target region split count after load", 1024},
  range_count = {"Disjoint ranges per query", 64},
  range_width = {"Row count per range", 50},
  rowid_stride = {"Distance between ranges", 50000},
}

local function must_query(c, sql)
  c:query(sql)
end

local function try_query(c, sql)
  local ok, err = pcall(function() c:query(sql) end)
  if not ok then
    print(string.format("WARN: SQL failed (ignored): %s | err=%s", sql, tostring(err)))
  end
end

local function build_scan_sql()
  local max_start = math.max(1, sysbench.opt.table_size - sysbench.opt.range_width - 1)
  local seed = sysbench.rand.uniform(1, max_start)
  local clauses = {}

  for i = 0, sysbench.opt.range_count - 1 do
    local start_id = ((seed + i * sysbench.opt.rowid_stride - 1) % max_start) + 1
    local end_id = start_id + sysbench.opt.range_width
    clauses[#clauses + 1] = string.format("(_tidb_rowid BETWEEN %d AND %d)", start_id, end_id)
  end

  return string.format(
    "SELECT SUM(t.k), COUNT(*) FROM %s t WHERE %s",
    sysbench.opt.table_name,
    table.concat(clauses, " OR ")
  )
end

local function new_connection()
  if drv == nil then
    drv = sysbench.sql.driver()
  end
  return drv:connect()
end

function thread_init()
  con = new_connection()
  must_query(con, "SET SESSION tidb_isolation_read_engines='tikv'")
  try_query(con, "SET SESSION tidb_allow_mpp=0")
end

function thread_done()
  con:disconnect()
end

function prepare()
  local prep_con = new_connection()
  local table_name = sysbench.opt.table_name

  must_query(prep_con, string.format("DROP TABLE IF EXISTS %s", table_name))
  must_query(prep_con, string.format(
    "CREATE TABLE %s (k BIGINT NOT NULL, pad VARBINARY(%d))",
    table_name,
    sysbench.opt.pad_size
  ))

  local batch = sysbench.opt.batch_size
  local total = sysbench.opt.table_size
  for begin_id = 1, total, batch do
    local end_id = math.min(begin_id + batch - 1, total)
    local values = {}
    for i = begin_id, end_id do
      values[#values + 1] = string.format("(%d, REPEAT('x', %d))", i % 1000000, sysbench.opt.pad_size)
    end
    must_query(
      prep_con,
      string.format("INSERT INTO %s (k, pad) VALUES %s", table_name, table.concat(values, ","))
    )
  end

  must_query(prep_con, string.format("ANALYZE TABLE %s", table_name))

  -- Increase cross-region probability.
  try_query(prep_con, "SET @@GLOBAL.tidb_scatter_region=1")
  try_query(prep_con, string.format(
    "SPLIT TABLE %s BETWEEN (1) AND (%d) REGIONS %d",
    table_name,
    total + 1,
    sysbench.opt.split_regions
  ))

  prep_con:disconnect()
end

function cleanup()
  local cleanup_con = new_connection()
  must_query(cleanup_con, string.format("DROP TABLE IF EXISTS %s", sysbench.opt.table_name))
  cleanup_con:disconnect()
end

function event()
  local sql = build_scan_sql()
  con:query(sql)
end
