-- drop table dqm.scenario
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
WHERE
sc.active_flag = 'Y'



SELECT
  sm.scenario_uid
, sm.measure_name
, sm.precision
, sm.allowed_variance
, sm.allowed_variance_rate
FROM
dqm.scenario_measure sm
WHERE
EXISTS (
  SELECT 1 FROM dqm.scenario s
  WHERE s.scenario_uid = sm.scenario_uid
  AND s.active_flag = 'Y'
)
;


select * from dqm.test order by test_index desc

-- truncate table dqm.test_case
select * from dqm.test_case
where test_uid = '4c70b268-eb48-4216-a0d1-376dfc038c32'
order by create_dtm desc

com.microsoft.sqlserver.jdbc.SQLServerException: The conversion from datetime to INTEGER is unsupported.
	at com.microsoft.sqlserver.jdbc.SQLServerException.makeFromDriverError(SQLServerException.java:190)
	at com.microsoft.sqlserver.jdbc.DataTypes.throwConversionError(DataTypes.java:1117)
	at com.microsoft.sqlserver.jdbc.ServerDTVImpl.getValue(dtv.java:2481)
	at com.microsoft.sqlserver.jdbc.DTV.getValue(dtv.java:193)
	at com.microsoft.sqlserver.jdbc.Column.getValue(Column.java:132)
	at com.microsoft.sqlserver.jdbc.SQLServerResultSet.getValue(SQLServerResultSet.java:2082)
	at com.microsoft.sqlserver.jdbc.SQLServerResultSet.getValue(SQLServerResultSet.java:2067)
	at com.microsoft.sqlserver.jdbc.SQLServerResultSet.getInt(SQLServerResultSet.java:2319)
	at com.centric.dqm.testing.Measure.assignExpectedValue(Measure.java:118)
	at com.centric.dqm.testing.Scenario.loadComparisons(Scenario.java:288)
	at com.centric.dqm.testing.Scenario.performTest(Scenario.java:116)
	at com.centric.dqm.testing.Harness.perfomTests(Harness.java:30)
	at com.centric.dqm.Application.main(Application.java:104)

