SELECT
  sc.scenario_uid
, sc.tag_list
, sc.grain_list
, sc.modulus
, sc.expected_connection_uid
, ec.jdbc_driver as expected_jdbc_driver
, ec.jdbc_url as expected_jdbc_url
, ec.username AS expected_username
, ec.password AS expected_password
, ec.timeout_ms AS expected_timeout_ms
, sc.expected_command
, sc.actual_connection_uid
, ac.jdbc_driver as actual_jdbc_driver
, ac.jdbc_url as actual_jdbc_url
, ac.username AS actual_username
, ac.password AS actual_password
, ac.timeout_ms AS actual_timeout_ms
, sc.actual_command
, sc.case_failure_record_limit
, sc.case_success_record_limit
, sc.allowed_case_failure_rate
FROM
dqm.scenario sc
LEFT JOIN dqm.connection ac ON ac.connection_uid = sc.actual_connection_uid
LEFT JOIN dqm.connection ec ON ec.connection_uid = sc.expected_connection_uid
WHERE
sc.active_flag = 'Y'
;