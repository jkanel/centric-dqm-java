USE [dqm]
GO




/* ############################################### */

DELETE FROM dqm.connection WHERE connection_uid IN ('AW2012','AWDW2012');
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

GO

/* ############################################### */

DELETE FROM dqm.scenario WHERE scenario_uid IN ('TEST1');
GO

DECLARE 
  @act VARCHAR(2000) = null
, @exp VARCHAR(2000) = null;

SET @exp = 'SELECT
  YEAR(h.OrderDate) AS year
, MONTH(h.OrderDate) AS month
, sp.StateProvinceCode AS state
, COUNT(DISTINCT h.SalesOrderID) AS order_count
, SUM(d.LineTotal) AS sales
, SUM(d.LineTotal)*1.0 / COUNT(DISTINCT h.SalesOrderID) * 2 AS sales_per_order
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
           ,[allowed_case_failure_rate]
           ,[active_flag]
           ,[create_dtm])

SELECT
'TEST1' AS [scenario_uid]
, 'Test 1' AS [scenario_desc]
, 'Axxxx,Bxxxx' AS [tag_list]
, 'year,month,state' AS [grain_list]
, 3 AS [modulus]
, 'AW2012' AS [expected_connection_uid]
, @exp AS [expected_command]
, 'AWDW2012' AS [actual_connection_uid]
, @act AS [actual_command]
,6 AS [case_failure_record_limit]
,4 AS [case_success_record_limit]
,0.05 AS [allowed_case_failure_rate]
,'Y' AS [active_flag]
,CURRENT_TIMESTAMP AS [create_dtm]


GO


/* ############################################### */

DELETE FROM dqm.[scenario_measure] WHERE scenario_uid = ('TEST1') AND measure_name = 'sales_per_order';
GO


INSERT INTO [dqm].[scenario_measure]
           ([scenario_uid]
           ,[measure_name]
           ,[precision]
           ,[allowed_variance]
           ,[allowed_variance_rate]
           ,[create_dtm])
SELECT
'TEST1' AS [scenario_uid]
,'sales_per_order' AS [measure_name]
,2 AS [precision]
,0.20 AS [allowed_variance]
,0.05[allowed_variance_rate]
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


