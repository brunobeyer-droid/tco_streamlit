/* P2.1 diagnostics - Team/Program scoped timeout investigation
   Safe to run read-only on SQL Server.
*/
SET NOCOUNT ON;

DECLARE @db SYSNAME = DB_NAME();

PRINT 'Database: ' + ISNULL(@db, '(unknown)');
PRINT 'UTC now: ' + CONVERT(VARCHAR(33), SYSUTCDATETIME(), 126);

/* 1) Currently running requests in this DB (focus on long/expensive statements) */
SELECT TOP (30)
  r.session_id,
  r.status,
  r.command,
  r.cpu_time,
  r.total_elapsed_time,
  r.logical_reads,
  r.reads,
  r.writes,
  r.wait_type,
  r.wait_time,
  r.blocking_session_id,
  DB_NAME(r.database_id) AS database_name,
  SUBSTRING(
    st.text,
    (r.statement_start_offset / 2) + 1,
    CASE
      WHEN r.statement_end_offset = -1 THEN LEN(CONVERT(NVARCHAR(MAX), st.text))
      ELSE (r.statement_end_offset - r.statement_start_offset) / 2 + 1
    END
  ) AS statement_text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
WHERE r.database_id = DB_ID(@db)
ORDER BY r.total_elapsed_time DESC;

/* 2) Top cached statements in this DB touching projected demand path */
SELECT TOP (50)
  qs.execution_count,
  CAST(qs.total_worker_time / 1000.0 AS DECIMAL(18,2)) AS total_cpu_ms,
  CAST(qs.total_elapsed_time / 1000.0 AS DECIMAL(18,2)) AS total_elapsed_ms,
  CAST((qs.total_elapsed_time / NULLIF(qs.execution_count, 0)) / 1000.0 AS DECIMAL(18,2)) AS avg_elapsed_ms,
  qs.total_logical_reads,
  qs.total_logical_writes,
  qs.last_execution_time,
  DB_NAME(COALESCE(st.dbid, qp.dbid)) AS database_name,
  LEFT(REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), st.text), CHAR(10), ' '), CHAR(13), ' '), 1200) AS sql_text_head
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
OUTER APPLY (SELECT CONVERT(INT, pa.value) AS dbid FROM sys.dm_exec_plan_attributes(qs.plan_handle) pa WHERE pa.attribute = 'dbid') qp
WHERE DB_NAME(COALESCE(st.dbid, qp.dbid)) = @db
  AND (
    st.text LIKE '%VW_TCO_FEATURE_DEMAND%'
    OR st.text LIKE '%TCO_TEAM_VELOCITY_SNAPSHOT%'
    OR st.text LIKE '%;WITH d AS (%'
    OR st.text LIKE '%DERIVED_FTE_FEATURE%'
  )
ORDER BY qs.total_elapsed_time DESC;

/* 3) Wait profile snapshot for this DB */
SELECT TOP (20)
  wt.wait_type,
  COUNT(*) AS waiting_tasks,
  SUM(wt.wait_duration_ms) AS total_wait_ms
FROM sys.dm_os_waiting_tasks wt
JOIN sys.dm_exec_requests r
  ON r.session_id = wt.session_id
WHERE r.database_id = DB_ID(@db)
GROUP BY wt.wait_type
ORDER BY total_wait_ms DESC;

/* 4) Existing indexes - key objects in projected path */
SELECT
  o.name AS object_name,
  i.name AS index_name,
  i.index_id,
  i.type_desc,
  i.is_unique,
  i.is_primary_key,
  i.fill_factor
FROM sys.indexes i
JOIN sys.objects o
  ON o.object_id = i.object_id
WHERE o.type = 'U'
  AND o.name IN (
    'ADO_FEATURES',
    'MAP_ADO_TEAM_TO_TCO_TEAM',
    'MAP_ADO_APP_TO_TCO_GROUP',
    'TEAMS',
    'PROGRAMS',
    'TCO_TEAM_VELOCITY_SNAPSHOT'
  )
ORDER BY o.name, i.index_id;

/* 5) Row counts (rough volume context) */
SELECT
  t.name AS table_name,
  SUM(p.rows) AS row_count
FROM sys.tables t
JOIN sys.partitions p
  ON p.object_id = t.object_id
WHERE p.index_id IN (0, 1)
  AND t.name IN (
    'ADO_FEATURES',
    'MAP_ADO_TEAM_TO_TCO_TEAM',
    'MAP_ADO_APP_TO_TCO_GROUP',
    'TCO_TEAM_VELOCITY_SNAPSHOT'
  )
GROUP BY t.name
ORDER BY row_count DESC;
