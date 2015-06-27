SELECT
  count(1) as result_count
FROM
sys.tables t 
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id

WHERE
t.name = 'scenario'
AND s.name = 'dqm';