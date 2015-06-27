USE [dqm]
GO

truncate table dqm.connection;
GO

INSERT INTO [dqm].[connection]
           ([connection_uid]
           ,[jdbc_driver]
           ,[jdbc_url]
           ,[username]
           ,[password]
           ,[timeout_ms]
           ,[create_dtm])
     VALUES
           ('TESTAW'
           ,'com.microsoft.sqlserver.jdbc.SQLServerDriver'
           ,'jdbc:sqlserver://localhost;databaseName=AdventureWorks2012'
           ,'centric'
           ,'centric'
           ,1000
           ,CURRENT_TIMESTAMP)
     
           , ('TESTAWDW'
           ,'com.microsoft.sqlserver.jdbc.SQLServerDriver'
           ,'jdbc:sqlserver://localhost;databaseName=AdventureWorksDW2012'
           ,'centric'
           ,'centric'
           ,1000
           ,CURRENT_TIMESTAMP)

GO

truncate table dqm.scenario

INSERT INTO [dqm].[scenario]
           ([scenario_uid]
           ,[scenario_desc]
           ,[tag_list]
           ,[grain_list]
           ,[modularity]
           ,[next_modulus]
           ,[expected_connection_uid]
           ,[expected_command]
           ,[actual_connection_uid]
           ,[actual_command]
           ,[override_expected_value]
           ,[preserve_failures_flag]
           ,[active_flag]
           ,[create_dtm])
     VALUES
           (
            'TEST1' -- [scenario_uid]
           ,'This is test 1' -- [scenario_desc]
           ,'testa, testb, testc' -- [tag_list]
           ,'year,month' -- [grain_list]
           ,3 -- [modularity]
           ,2 -- [next_modulus]
           ,'TESTAW' -- [expected_connection_uid]
           ,null -- [expected_command]
           ,'TESTAWDW' -- [actual_connection_uid]
           ,null -- [actual_command]
           ,NULL -- [override_expected_value]
           ,'Y' -- [preserve_failures_flag]
           ,'Y' -- [active_flag]
           ,CURRENT_TIMESTAMP -- [create_dtm]
           )

        , (
            'TEST2' -- [scenario_uid]
           ,'This is test 2' -- [scenario_desc]
           ,'testb, testc' -- [tag_list]
           ,'resellerkey, year' -- [grain_list]
           ,7 -- [modularity]
           ,2 -- [next_modulus]
           ,'TESTAW' -- [expected_connection_uid]
           ,null -- [expected_command]
           ,'TESTAWDW' -- [actual_connection_uid]
           ,null -- [actual_command]
           ,NULL -- [override_expected_value]   
           ,'Y' -- [preserve_failures_flag]
           ,'Y' -- [active_flag]
           ,CURRENT_TIMESTAMP -- [create_dtm]
           )
GO

