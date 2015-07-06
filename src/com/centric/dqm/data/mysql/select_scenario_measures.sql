SELECT
  sm.scenario_uid
, sm.measure_name
, sm.precision
, sm.allowed_variance
, sm.allowed_variance_rate
, sm.flexible_null_equality_flag
FROM
dqm.scenario_measure sm
WHERE
EXISTS (
  SELECT 1 FROM dqm.scenario s
  WHERE s.scenario_uid = sm.scenario_uid
  AND s.active_flag = 'Y'
)
;