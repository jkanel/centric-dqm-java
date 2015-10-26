IF NOT EXISTS (SELECT  1 FROM sys.schemas WHERE name = 'dqm' ) 
EXEC sp_executesql N'CREATE SCHEMA dqm'
;

IF OBJECT_ID('dqm.scenario') IS NOT NULL
DROP TABLE dqm.scenario
;

IF OBJECT_ID('dqm.scenario_measure') IS NOT NULL
DROP TABLE dqm.scenario_measure
;

IF OBJECT_ID('dqm.connection') IS NOT NULL
DROP TABLE dqm.connection
;

IF OBJECT_ID('dqm.test') IS NOT NULL
DROP TABLE dqm.test
;

IF OBJECT_ID('dqm.test_case') IS NOT NULL
DROP TABLE dqm.test_case
;

CREATE TABLE dqm.scenario (
  scenario_uid varchar(200) NOT NULL
, scenario_desc varchar(2000)
, tag_list varchar(2000)
, grain_list varchar(2000)
, modulus int default 1 -- inverse fraction of data to consider in a given test
, expected_connection_uid varchar(200)
, expected_command varchar(max)
, actual_connection_uid varchar(200)
, actual_command varchar(max)
, case_failure_record_limit int NOT NULL DEFAULT 100 -- number of success values to preserve
, case_success_record_limit int NOT NULL DEFAULT 0  -- number of success values to preserve
, allowed_case_failure_rate float NOT NULL DEFAULT 0.0  -- number of results allowed to fail
, flexible_null_equality_flag CHAR(1)
, active_flag char(1) DEFAULT 'Y' NOT NULL 
, create_dtm datetime DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT scenario_pk PRIMARY KEY (scenario_uid)
)
;

CREATE TABLE dqm.scenario_measure (
  scenario_uid varchar(200) NOT NULL
, measure_name varchar(200) NOT NULL
, precision int -- number of significant digits used to evaulat the variance
, flexible_null_equality_flag CHAR(1)
, allowed_variance float
, allowed_variance_rate float
, create_dtm datetime DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT scenario_measure_pk PRIMARY KEY (scenario_uid, measure_name)
)

CREATE TABLE dqm.connection (
  connection_uid varchar(200) NOT NULL
, jdbc_driver varchar(200)
, jdbc_url varchar(2000)
, username varchar(200)
, password varchar(200)
, timeout_sec int default -1
, create_dtm datetime DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT connection_pk PRIMARY KEY (connection_uid)
)
;

CREATE TABLE dqm.test (
  test_uid varchar(200) NOT NULL
, scenario_uid varchar(200) NOT NULL
, test_begin_dtm datetime
, test_end_dtm datetime
, expected_exec_begin_dtm datetime
, expected_exec_end_dtm datetime
, actual_exec_begin_dtm datetime
, actual_exec_end_dtm datetime
, modularity int
, modulus int
, failure_case_ct int
, success_case_ct int
, case_failure_rate float
, allowed_case_failure_rate float
, failure_flag char(1)
, error_flag char(1) -- indicates that there was an execution error
, test_error_message varchar(2000)
, expected_error_message varchar(2000)
, actual_error_message varchar(2000)
, test_index int IDENTITY(0,1) not null -- auto incrementing
, test_case_purge_dtm datetime NULL
, create_dtm datetime DEFAULT CURRENT_TIMESTAMP
, CONSTRAINT test_pk PRIMARY KEY (test_uid)
)
;

CREATE UNIQUE INDEX test_u1 ON dqm.test (test_index)
;


CREATE TABLE dqm.test_case (
  test_uid varchar(200) NOT NULL
, measure_name varchar(200) NOT NULL
, precision int
, expected_value float
, expected_text varchar(200)
, actual_value float
, actual_text varchar(200)
, result_variance float
, result_variance_rate float
, allowed_variance float
, allowed_variance_rate float 
, failure_flag char(1)
, grain_hash varchar(2000)
, grain_01_name varchar(200)
, grain_01_text varchar(200)
, grain_02_name varchar(200)
, grain_02_text varchar(200)
, grain_03_name varchar(200)
, grain_03_text varchar(200)
, grain_04_name varchar(200)
, grain_04_text varchar(200)
, grain_05_name varchar(200)
, grain_05_text varchar(200)
, create_dtm datetime DEFAULT CURRENT_TIMESTAMP
)
;