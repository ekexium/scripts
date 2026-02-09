-- RowID multi-range coprocessor stress for TiDB/CSE
-- Target: generate many disjoint _tidb_rowid ranges per query, crossing many regions.

local drv
local con
local split_con
local writer_con

sysbench.cmdline.options = {
  table_name = {"Table name", "sbtest_rowid_scan"},
  table_size = {"Rows to load", 5000000},
  batch_size = {"Rows per INSERT batch", 1000},
  pad_size = {"Bytes for padding column", 64},
  split_regions = {"Target region split count after load", 1024},
  split_during_run = {"Issue periodic SPLIT TABLE during run (0/1)", 0},
  split_every = {"SPLIT TABLE every N events (split threads)", 200},
  split_range_width = {"RowID width per split (0=full table)", 0},
  split_regions_per_op = {"Regions per split op (capped by TiDB)", 1024},

  writer_threads = {"Writer threads during run", 8},
  writer_rows_per_event = {"Rows to append per writer event", 50},
  writer_flush_every = {"Flush every N appended rows (bulk insert)", 1000},

  scan_tail_window = {"Bias scan seed to tail N rows", 2000000},
  scan_tail_extra = {"Max rowid extra above table_size", 8000000},

  scan_error_abort = {"Abort on scan error (0/1)", 1},
  scan_error_max_print = {"Print at most N scan errors per thread", 20},

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

local function set_scatter_region_mode(c)
  local candidates = {
    "SET @@GLOBAL.tidb_scatter_region='table'",
    "SET @@GLOBAL.tidb_scatter_region=1",
  }

  local last_err
  for _, sql in ipairs(candidates) do
    local ok, err = pcall(function() c:query(sql) end)
    if ok then
      return
    end
    last_err = err
  end

  print(string.format(
    "WARN: failed to set tidb_scatter_region via all candidates, keep default | err=%s",
    tostring(last_err)
  ))
end

local function split_table_with_fallback(c, table_name, start_rowid, end_rowid, requested_regions)
  local split_sql = string.format(
    "SPLIT TABLE %s BETWEEN (%d) AND (%d) REGIONS %d",
    table_name,
    start_rowid,
    end_rowid,
    requested_regions
  )

  local ok, err = pcall(function() c:query(split_sql) end)
  if ok then
    return
  end

  local err_msg = tostring(err)
  local limit = tonumber(string.match(err_msg, "exceeded the limit%s+(%d+)"))
  if limit ~= nil and requested_regions > limit then
    print(string.format(
      "WARN: requested split regions %d exceeds TiDB limit %d, retry with %d",
      requested_regions,
      limit,
      limit
    ))
    local retry_sql = string.format(
      "SPLIT TABLE %s BETWEEN (%d) AND (%d) REGIONS %d",
      table_name,
      start_rowid,
      end_rowid,
      limit
    )
    local ok_retry, err_retry = pcall(function() c:query(retry_sql) end)
    if ok_retry then
      return
    end
    print(string.format("WARN: SQL failed (ignored): %s | err=%s", retry_sql, tostring(err_retry)))
    return
  end

  print(string.format("WARN: SQL failed (ignored): %s | err=%s", split_sql, err_msg))
end

local function should_split_in_this_thread()
  if sysbench.opt.split_during_run ~= 1 then
    return false
  end

  local tid = rawget(sysbench, "tid")
  if tid == nil then
    return true
  end
  return tid == 0
end

local function get_thread_id()
  if thread_id ~= nil then
    return thread_id
  end
  local tid = rawget(sysbench, "tid")
  if tid ~= nil then
    return tid
  end
  return 0
end

local function is_writer_thread()
  return get_thread_id() < sysbench.opt.writer_threads
end

local function build_scan_sql()
  local max_rowid = sysbench.opt.table_size + sysbench.opt.scan_tail_extra
  local max_seed = math.max(
    1,
    max_rowid - (sysbench.opt.range_count - 1) * sysbench.opt.rowid_stride - sysbench.opt.range_width - 1
  )
  local tail_start = math.max(1, max_rowid - sysbench.opt.scan_tail_window)
  if tail_start > max_seed then
    tail_start = 1
  end
  local seed = sysbench.rand.uniform(tail_start, max_seed)
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
  split_enabled = should_split_in_this_thread()
  split_event_counter = 0

  if is_writer_thread() then
    writer_con = new_connection()
    writer_con:bulk_insert_init(string.format(
      "INSERT INTO %s (k, pad) VALUES",
      sysbench.opt.table_name
    ))
    writer_pending_rows = 0
    return
  end

  con = new_connection()
  must_query(con, "SET SESSION tidb_isolation_read_engines='tikv'")
  try_query(con, "SET SESSION tidb_allow_mpp=0")
  scan_error_count = 0
  scan_error_printed = 0
  if split_enabled then
    split_con = new_connection()
  end
end

function thread_done()
  if writer_con ~= nil then
    if writer_pending_rows ~= nil and writer_pending_rows > 0 then
      writer_con:bulk_insert_done()
    end
    writer_con:disconnect()
    return
  end

  if scan_error_count ~= nil and scan_error_count > 0 then
    print(string.format("WARN: scan errors in thread=%d count=%d", get_thread_id(), scan_error_count))
  end

  con:disconnect()
  if split_con ~= nil then
    split_con:disconnect()
  end
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
  set_scatter_region_mode(prep_con)
  split_table_with_fallback(prep_con, table_name, 1, total + 1, sysbench.opt.split_regions)

  prep_con:disconnect()
end

function cleanup()
  local cleanup_con = new_connection()
  must_query(cleanup_con, string.format("DROP TABLE IF EXISTS %s", sysbench.opt.table_name))
  cleanup_con:disconnect()
end

function event()
  if writer_con ~= nil then
    for _ = 1, sysbench.opt.writer_rows_per_event do
      writer_con:bulk_insert_next(string.format(
        "(%d, REPEAT('x', %d))",
        sysbench.rand.uniform(1, 1000000),
        sysbench.opt.pad_size
      ))
      writer_pending_rows = writer_pending_rows + 1
      if sysbench.opt.writer_flush_every > 0 and writer_pending_rows >= sysbench.opt.writer_flush_every then
        writer_con:bulk_insert_done()
        writer_con:bulk_insert_init(string.format(
          "INSERT INTO %s (k, pad) VALUES",
          sysbench.opt.table_name
        ))
        writer_pending_rows = 0
      end
    end
    return
  end

  if split_enabled then
    split_event_counter = split_event_counter + 1
    if sysbench.opt.split_every > 0 and (split_event_counter % sysbench.opt.split_every == 0) then
      local max_rowid = sysbench.opt.table_size + 1
      local start_rowid = 1
      local end_rowid = max_rowid
      local width = sysbench.opt.split_range_width
      if width ~= nil and width > 0 and width < max_rowid then
        start_rowid = sysbench.rand.uniform(1, max_rowid - width)
        end_rowid = start_rowid + width
      end
      split_table_with_fallback(
        split_con,
        sysbench.opt.table_name,
        start_rowid,
        end_rowid,
        sysbench.opt.split_regions_per_op
      )
    end
  end

  local sql = build_scan_sql()
  local ok, err = pcall(function() con:query(sql) end)
  if ok then
    return
  end

  scan_error_count = (scan_error_count or 0) + 1
  if (scan_error_printed or 0) < sysbench.opt.scan_error_max_print then
    scan_error_printed = (scan_error_printed or 0) + 1
    print(string.format("SCAN_ERROR: %s | err=%s", sql, tostring(err)))
  end
  if sysbench.opt.scan_error_abort == 1 then
    error(err)
  end
end
