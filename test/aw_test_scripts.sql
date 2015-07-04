USE [dqm]
GO

-- drop table dqm.scenario


/* ############################################### */

DELETE FROM dqm.connection;
GO

INSERT INTO [dqm].[connection]
           ([connection_uid]
           ,[jdbc_driver]
           ,[jdbc_url]
           ,[username]
           ,[password]
           ,[timeout_sec]
           ,[create_dtm])

SELECT
'AW2012' AS [connection_uid]
, 'com.microsoft.sqlserver.jdbc.SQLServerDriver' AS [jdbc_driver]
, 'jdbc:sqlserver://localhost;databaseName=AdventureWorks2012' AS [jdbc_url]
, 'centric' AS [username]
, 'centric' AS [password]
, 5 AS [timeout_Sec]
,CURRENT_TIMESTAMP AS [create_dtm]

UNION ALL

SELECT
'AWDW2012' AS [connection_uid]
, 'com.microsoft.sqlserver.jdbc.SQLServerDriver' AS [jdbc_driver]
, 'jdbc:sqlserver://localhost;databaseName=AdventureWorksDW2012' AS [jdbc_url]
, 'centric' AS [username]
, 'centric' AS [password]
, 5 AS [timeout_sec]
,CURRENT_TIMESTAMP AS [create_dtm]

UNION ALL

SELECT
'CAP_DW' AS [connection_uid]
, 'com.microsoft.sqlserver.jdbc.SQLServerDriver' AS [jdbc_driver]
, 'jdbc:sqlserver://172.16.2.7;databaseName=warehouse' AS [jdbc_url]
, 'centric_developer' AS [username]
, 'centric_developer' AS [password]
, 5 AS [timeout_sec]
,CURRENT_TIMESTAMP AS [create_dtm]


UNION ALL

SELECT
'CAP_CEDW' AS [connection_uid]
, 'com.microsoft.sqlserver.jdbc.SQLServerDriver' AS [jdbc_driver]
, 'jdbc:sqlserver://172.16.2.7;databaseName=CentricEnterpriseDW' AS [jdbc_url]
, 'centric_developer' AS [username]
, 'centric_developer' AS [password]
, 5 AS [timeout_sec]
,CURRENT_TIMESTAMP AS [create_dtm]


GO


/* ############################################### */

DELETE FROM dqm.scenario;
GO

DECLARE 
  @act VARCHAR(max) = null
, @exp VARCHAR(max) = null;

SET @exp = 'dbo.dqm_test @modulus=<<MODULUS>>, @modularity=<<MODULARITY>>;'

/*
'SELECT
  YEAR(h.OrderDate) AS year
, MONTH(h.OrderDate) AS month
, sp.StateProvinceCode AS state
, COUNT(DISTINCT h.SalesOrderID) AS order_count
, SUM(d.LineTotal) * 1.2 AS sales
, SUM(d.LineTotal)*1.0 / COUNT(DISTINCT h.SalesOrderID) AS sales_per_order
, MAX(h.SalesOrderNumber) AS max_order_number
, MIN(h.OrderDate) AS min_order_date
FROM
Sales.SalesOrderHeader h
INNER JOIN Sales.SalesOrderDetail d ON d.SalesOrderID = h.SalesOrderID
INNER JOIN Person.Address a ON a.AddressID = h.BillToAddressID
INNER JOIN Person.StateProvince sp ON sp.StateProvinceID = a.StateProvinceID
WHERE
sp.CountryRegionCode = ''US''
AND YEAR(h.OrderDate) >= YEAR(CURRENT_TIMESTAMP)-8
AND Month(h.OrderDate) % <<MODULUS>> = <<MODULARITY>>
AND h.OnlineOrderFlag = 1
GROUP BY
  YEAR(h.OrderDate)
, MONTH(h.OrderDate)
, sp.StateProvinceCode
ORDER BY
  YEAR(h.OrderDate)
, MONTH(h.OrderDate)
, sp.StateProvinceCode';
*/

SET @act = 'SELECT
YEAR(s.OrderDate) AS year
, MONTH(s.OrderDate) AS month
, g.StateProvinceCode AS state
, COUNT(DISTINCT s.SalesOrderNumber) AS order_count
, SUM(s.SalesAmount) AS sales
, SUM(s.SalesAmount)*1.0/COUNT(DISTINCT s.SalesOrderNumber) AS sales_per_order
, MAX(s.SalesOrderNumber) AS max_order_number
, MIN(s.OrderDate) AS min_order_date
FROM
FactInternetSales s
INNER JOIN DimDate d ON d.DateKey = s.OrderDateKey
INNER JOIN DimCustomer c ON c.CustomerKey = s.CustomerKey
INNER JOIN DimGeography g ON g.GeographyKey = c.GeographyKey
WHERE
g.CountryRegionCode = ''US''
AND YEAR(s.OrderDate) >= YEAR(CURRENT_TIMESTAMP)-8
AND Month(s.OrderDate) % <<MODULUS>> = <<MODULARITY>>
GROUP BY
  YEAR(s.OrderDate)
, MONTH(s.OrderDate)
, g.StateProvinceCode
ORDER BY
  YEAR(s.OrderDate)
, MONTH(s.OrderDate)
, g.StateProvinceCode'

INSERT INTO [dqm].[scenario]
           ([scenario_uid]
           ,[scenario_desc]
           ,[tag_list]
           ,[grain_list]
           ,[modulus]
           ,[expected_connection_uid]
           ,[expected_command]
           ,[actual_connection_uid]
           ,[actual_command]
           ,[case_failure_record_limit]
           ,[case_success_record_limit]
           ,[flexible_null_equality_flag]
           ,[allowed_case_failure_rate]
           ,[active_flag]
           ,[create_dtm])

SELECT
  'TEST1' AS [scenario_uid]
, 'Test 1' AS [scenario_desc]
, 'TEST' AS [tag_list]
, 'year,month,state' AS [grain_list]
, 3 AS [modulus]
, 'AW2012' AS [expected_connection_uid]
, @exp AS [expected_command]
, 'AWDW2012' AS [actual_connection_uid]
, @act AS [actual_command]
, 1000 AS [case_failure_record_limit]
, 0 AS [case_success_record_limit]
, 'Y' AS [flexible_null_equality_flag]
, 0.05 AS [allowed_case_failure_rate]
, 'Y' AS [active_flag]
, CURRENT_TIMESTAMP AS [create_dtm]

GO

DECLARE 
  @act VARCHAR(max) = null
, @exp VARCHAR(max) = null;

SET @exp = '
SELECT
  x.year
, x.month
, x.operating_group

, x.net_amt
, x.income_amt
, x.expense_amt
, x.net_expense_amt
, x.net_revenue_amt
, x.cogs_amt
  -- supress zero aggregates, replace with NULL
, CASE WHEN x.net_cogs_amt = 0.0 THEN NULL ELSE x.net_cogs_amt END AS net_cogs_amt
, x.reimb_expense_income_amt
, x.reimb_expense_cost_amt
, x.employee_cost_amt
, x.contractor_cost_amt
, x.w2t_cost_amt
, x.gross_profit_amt
, x.non_cogs_amt  
, x.net_profit_amt
, x.centric_cost_amt
, x.centric_cost_transfer_amt
  -- supress zero aggregates, replace with NULL
, CASE WHEN x.net_centric_cost_amt = 0.0 THEN NULL ELSE x.net_centric_cost_amt END AS net_centric_cost_amt
, x.bu_cost_amt
  -- supress zero aggregates, replace with NULL
, CASE WHEN x.operating_profit_amt = 0.0 THEN NULL ELSE x.operating_profit_amt END AS operating_profit_amt
, x.shared_profit_transfer_amt
FROM
(

  SELECT
    year
  , month
  , BU AS operating_group

  , SUM(NetAmt) AS net_amt
  , SUM(CASE WHEN Category = ''Income'' THEN NetAmt END) AS income_amt
  , -1*SUM(CASE WHEN Category = ''COGS'' and RollupAccount=''Expenses'' THEN NetAmt END) AS expense_amt
  , -1*SUM(CASE WHEN RollupAccount=''Expenses'' THEN NetAmt END) AS net_expense_amt
  , SUM(CASE WHEN RollupAccount = ''Revenue'' THEN NetAmt END) AS net_revenue_amt
  , -1*SUM(CASE WHEN Category = ''COGS'' THEN NetAmt END) AS cogs_amt

  , -1*(

      ISNULL(SUM(CASE WHEN Category = ''COGS'' THEN NetAmt END), 0)
      - ISNULL(SUM(CASE WHEN Category = ''Income'' and RollupAccount=''Expenses'' THEN NetAmt END), 0)

     ) AS net_cogs_amt

  , SUM(CASE WHEN Category = ''Income'' and RollupAccount=''Expenses'' THEN NetAmt END) AS reimb_expense_income_amt
  , -1*SUM(CASE WHEN Category = ''Income'' and RollupAccount=''Expenses'' THEN NetAmt END) AS reimb_expense_cost_amt
  , -1*SUM(CASE WHEN RollupAccount IN (''PartnerWages'',''Employees'') THEN NetAmt END) AS employee_cost_amt
  , -1*SUM(CASE WHEN RollupAccount = ''Contractors and W2T'' AND Account=''Contractors-COGS'' THEN NetAmt END) AS contractor_cost_amt
  , -1*SUM(CASE WHEN RollupAccount in (''Contractors and W2T'') AND Account != ''Contractors-COGS'' THEN NetAmt END) AS w2t_cost_amt
  , SUM(CASE WHEN  Category IN (''Income'',''COGS'') THEN NetAmt END) AS gross_profit_amt
  , -1*SUM(CASE WHEN Category = ''Non-COGS'' THEN NetAmt END)  AS non_cogs_amt 
  , SUM(NetAmt) AS net_profit_amt
  , -1* SUM(CASE WHEN Category=''Non-COGS'' AND BU IN (''National'',''Shared Services'') THEN NetAmt END) AS centric_cost_amt
  , -1* SUM(CASE WHEN RollupAccount=''National Cost Allocation'' AND BU IN (''National'',''Shared Services'') THEN NetAmt END) AS centric_cost_transfer_amt

  , -1*(

      ISNULL(SUM(CASE WHEN RollupAccount=''National Cost Allocation'' AND BU NOT IN (''National'',''Shared Services'') THEN NetAmt END), 0)
      - ISNULL(SUM(CASE WHEN RollupAccount=''National Cost Allocation'' AND BU IN (''National'',''Shared Services'') THEN NetAmt END), 0)

    ) AS net_centric_cost_amt

  , -1*SUM(CASE WHEN Category = ''Non-COGS'' AND BU NOT IN (''National'',''Shared Services'') THEN NetAmt END) AS bu_cost_amt
  , (
  
      ISNULL(SUM(CASE WHEN  Category IN (''Income'',''COGS'') THEN NetAmt END), 0)
      - ISNULL(SUM(CASE WHEN Category = ''Non-COGS'' THEN NetAmt END), 0)

    ) AS operating_profit_amt
  , SUM(CASE WHEN RollupAccount = ''Shared Profit -Resource Share'' THEN NetAmt END) AS shared_profit_transfer_amt

  FROM
  CentricEnterpriseDW.dbo.QB_PL_Detail
  WHERE
  Year >= 2010 AND Year <= 2015
 
  GROUP BY
    year
  , month
  , BU

) x
'

SET @act = '
SELECT
  c.year
, c.month_of_year AS month
, og.operating_group_desc AS operating_group

, SUM(x.net_amt) AS net_amt
, SUM(x.income_amt) AS income_amt
, SUM(x.expense_amt) AS expense_amt
, SUM(x.net_expense_amt) AS net_expense_amt
, SUM(x.net_revenue_amt) AS net_revenue_amt
, SUM(x.cogs_amt) AS cogs_amt
, SUM(x.net_cogs_amt) AS  net_cogs_amt
, SUM(x.reimb_expense_income_amt) AS reimb_expense_income_amt
, SUM(x.reimb_expense_cost_amt) AS  reimb_expense_cost_amt
, SUM(x.employee_cost_amt) AS employee_cost_amt
, SUM(x.contractor_cost_amt) AS contractor_cost_amt
, SUM(x.w2t_cost_amt) AS w2t_cost_amt
, SUM(x.gross_profit_amt) AS gross_profit_amt
, SUM(x.non_cogs_amt) AS   non_cogs_amt
, SUM(x.net_profit_amt) AS net_profit_amt
, SUM(x.centric_cost_amt) AS centric_cost_amt
, SUM(x.centric_cost_transfer_amt) AS centric_cost_transfer_amt
  -- supress zero aggregates, replace with NULL
, SUM(x.net_centric_cost_amt) AS net_centric_cost_amt
, SUM(x.bu_cost_amt) AS  bu_cost_amt
  -- supress zero aggregates, replace with NULL
, SUM(x.operating_profit_amt) AS  operating_profit_amt
, SUM(x.shared_profit_transfer_amt) AS  shared_profit_transfer_amt

FROM
gl_tran x
INNER JOIN dbo.operating_group og ON og.operating_group_key = x.operating_group_key
INNER JOIN dbo.calendar c ON c.date_key = x.gl_tran_date_key
WHERE
c.year >= 2010
GROUP BY
  c.year
, c.month_of_year
, og.operating_group_desc
'

INSERT INTO [dqm].[scenario]
           ([scenario_uid]
           ,[scenario_desc]
           ,[tag_list]
           ,[grain_list]
           ,[modulus]
           ,[expected_connection_uid]
           ,[expected_command]
           ,[actual_connection_uid]
           ,[actual_command]
           ,[case_failure_record_limit]
           ,[case_success_record_limit]
           ,[allowed_case_failure_rate]
           ,[active_flag]
           ,[create_dtm])

SELECT
  'CAP_GL1' AS [scenario_uid]
, 'CAP_GL1' AS [scenario_desc]
, 'CAP' AS [tag_list]
, 'year,month,operating_group' AS [grain_list]
, 1 AS [modulus]
, 'CAP_CEDW' AS [expected_connection_uid]
, @exp AS [expected_command]
, 'CAP_DW' AS [actual_connection_uid]
, @act AS [actual_command]
, 2000 AS [case_failure_record_limit]
, 0 AS [case_success_record_limit]
, 0.00 AS [allowed_case_failure_rate]
, 'Y' AS [active_flag]
, CURRENT_TIMESTAMP AS [create_dtm]

GO


/* ############################################### */

DELETE FROM dqm.[scenario_measure];
GO


INSERT INTO [dqm].[scenario_measure]
           ([scenario_uid]
           ,[measure_name]
           ,[precision]
           ,[allowed_variance]
           ,[allowed_variance_rate]
           , [flexible_null_equality_flag]
           ,[create_dtm])
SELECT
'TEST1' AS [scenario_uid]
,'sales_per_order' AS [measure_name]
,2 AS [precision]
,0.20 AS [allowed_variance]
,0.05[allowed_variance_rate]
, 'N' AS [flexible_null_equality_flag]
, CURRENT_TIMESTAMP AS [create_dtm]

GO


/*

-- expected
USE AdventureWorks2012

SELECT
  YEAR(h.OrderDate) AS year
, MONTH(h.OrderDate) AS month
, sp.StateProvinceCode AS state
, COUNT(DISTINCT h.SalesOrderID) AS order_count
, SUM(d.LineTotal) AS sales
, SUM(d.LineTotal)*1.0 / COUNT(DISTINCT h.SalesOrderID) AS sales_per_order
, MAX(h.SalesOrderNumber) AS max_order_number
, MIN(h.OrderDate) AS min_order_date
FROM
Sales.SalesOrderHeader h
INNER JOIN Sales.SalesOrderDetail d ON d.SalesOrderID = h.SalesOrderID
INNER JOIN Person.Address a ON a.AddressID = h.BillToAddressID
INNER JOIN Person.StateProvince sp ON sp.StateProvinceID = a.StateProvinceID
WHERE
sp.CountryRegionCode = 'US'
AND YEAR(h.OrderDate) >= YEAR(CURRENT_TIMESTAMP)-8
AND Month(h.OrderDate) % 2 = 0
AND h.OnlineOrderFlag = 1
GROUP BY
  YEAR(h.OrderDate)
, MONTH(h.OrderDate)
, sp.StateProvinceCode
ORDER BY
  YEAR(h.OrderDate)
, MONTH(h.OrderDate)
, sp.StateProvinceCode
;

USE AdventureWorksDW2012
GO

SELECT
YEAR(s.OrderDate) AS year
, MONTH(s.OrderDate) AS month
, g.StateProvinceCode AS state
, COUNT(DISTINCT s.SalesOrderNumber) AS order_count
, SUM(s.SalesAmount) AS sales
, SUM(s.SalesAmount)*1.0/COUNT(DISTINCT s.SalesOrderNumber) AS sales_per_order
, MAX(s.SalesOrderNumber) AS max_order_number
, MIN(s.OrderDate) AS min_order_date
FROM
FactInternetSales s
INNER JOIN DimDate d ON d.DateKey = s.OrderDateKey
INNER JOIN DimCustomer c ON c.CustomerKey = s.CustomerKey
INNER JOIN DimGeography g ON g.GeographyKey = c.GeographyKey
WHERE
g.CountryRegionCode = 'US'
AND YEAR(s.OrderDate) >= YEAR(CURRENT_TIMESTAMP)-8
AND Month(s.OrderDate) % 2 = 0
GROUP BY
  YEAR(s.OrderDate)
, MONTH(s.OrderDate)
, g.StateProvinceCode
ORDER BY
  YEAR(s.OrderDate)
, MONTH(s.OrderDate)
, g.StateProvinceCode
;

*/



truncate table dqm.test
truncate table dqm.test_case

select * from dqm.scenario