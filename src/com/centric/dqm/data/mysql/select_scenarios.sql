SELECT
  sc.scenario_uid
, sc.tag_list
, sc.grain_list
, sc.modulus
, CASE  
  WHEN t.modularity IS NULL THEN 0
  WHEN t.modulus != sc.modulus THEN 0
  WHEN t.modularity + 1 >= sc.modulus THEN 0
  ELSE t.modularity + 1
  END AS modularity
, sc.expected_connection_uid
, ec.jdbc_driver as expected_jdbc_driver
, ec.jdbc_url as expected_jdbc_url
, ec.username AS expected_username
, ec.password AS expected_password
, ec.timeout_sec AS expected_timeout_sec
, sc.expected_command
, sc.actual_connection_uid
, ac.jdbc_driver as actual_jdbc_driver
, ac.jdbc_url as actual_jdbc_url
, ac.username AS actual_username
, ac.password AS actual_password
, ac.timeout_sec AS actual_timeout_sec
, sc.actual_command
, sc.case_failure_record_limit
, sc.case_success_record_limit
, sc.allowed_case_failure_rate
, sc.flexible_null_equality_flag
, sc.active_flag
FROM
dqm.scenario sc
LEFT JOIN dqm.connection ac ON ac.connection_uid = sc.actual_connection_uid
LEFT JOIN dqm.connection ec ON ec.connection_uid = sc.expected_connection_uid
LEFT JOIN (

  SELECT
    tx.scenario_uid
  , MAX(tx.test_index) AS last_test_index  
  FROM
  dqm.test tx
  GROUP BY
  tx.scenario_uid

) tm ON tm.scenario_uid = sc.scenario_uid
LEFT JOIN dqm.test t ON t.test_index = tm.last_test_index

;