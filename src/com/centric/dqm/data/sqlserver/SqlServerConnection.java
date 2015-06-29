package com.centric.dqm.data.sqlserver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.FilteredRowSet;

import org.apache.commons.lang3.StringUtils;

import com.centric.dqm.Application;
import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.IConnection;
import com.centric.dqm.data.RowsetStringFilter;
import com.centric.dqm.testing.Grain;
import com.centric.dqm.testing.Harness;
import com.centric.dqm.testing.Measure;
import com.centric.dqm.testing.Scenario;
import com.centric.dqm.testing.TestCase;
import com.sun.rowset.FilteredRowSetImpl;

public class SqlServerConnection implements IConnection  {

	public final static String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public final static String SCRIPT_RESOURCE_FOLDER = "sqlserver";
	
	public String user = null;
	public String password = null;
	public String jdbcUrl = null;
	public int timeout = -1;

	
	public SqlServerConnection(){}

	public SqlServerConnection(String jdbcUrl)
	{
		this.jdbcUrl = jdbcUrl;
	}
		
	public SqlServerConnection(Properties properties)
	{
		this.applyProperties(properties);
	}
	
	public void applyProperties(Properties properties)
	{
		this.jdbcUrl = properties.getProperty("url");
		this.user = properties.getProperty("user");
		this.password = properties.getProperty("password");
		
		if(properties.containsKey("timeout")==true)
		{
			this.timeout = Integer.parseInt(properties.getProperty("timeout"));
		}
		
		// check for missing properties and throw error if needed
		List<String> errorList = new ArrayList<String>();
		
		if(this.user.length() == 0)
		{
			errorList.add("user");
		}
		
		if(this.password.length() == 0)
		{
			errorList.add("password");
		}
		
		if(this.jdbcUrl.length() == 0)
		{
			errorList.add("url");
		}
		
		if(errorList.size() > 0)
		{
			throw new IllegalArgumentException("The following properties must be specified: " + StringUtils.join(errorList, ","));
		}
				
	}
	
	public String getConnectionUrl()
	{		
		return this.jdbcUrl;
	}
	
	public String getJdbcDriver(){
		return SqlServerConnection.JDBC_DRIVER;
	}
	
	public String getConnectionUser()
	{
		return this.user;
	}
	
	public String getConnectionPassword()
	{
		return this.password;		
	}

	public int getConnectionTimeout()
	{
		return this.timeout;		
	}
	
	public ResultSet executeCommandWithResult(String commandText)
	{
	    return DataUtils.executeCommandWithResult(commandText, this);	    
	}
	
	public void executeCommand(String commandText)
	{
	    DataUtils.executeCommand(commandText, this);	    
	}
	
	public void readHarness(Harness harness) throws Exception
	{
		
		String commandText;
		ResultSet srs = null;
		ResultSet smrs = null;
		Scenario sc = null;
		RowsetStringFilter scenarioFilter;
		Measure m;
		
		// execute the test for existing tables
		commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.SELECT_SCENARIOS_RESOURCE);	
		srs = DataUtils.executeCommandWithResult(commandText, this);
		
		commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.SELECT_SCENARIO_MEASURES_RESOURCE);	
		smrs = DataUtils.executeCommandWithResult(commandText, this);
		
		// push the measure scenarios into a filtered rowset
		FilteredRowSet frs = new FilteredRowSetImpl();
		frs.populate(smrs);
		
		DataUtils.disposeResulset(smrs);

		while(srs.next())
		{
			sc = new Scenario();
			
			sc.identifier = srs.getString("scenario_uid");		
			
			sc.actualConnection = DataUtils.getConnectionFromDriver(
					srs.getString("actual_jdbc_driver"), 
					srs.getString("actual_jdbc_url"), 
					srs.getString("actual_username"), 
					srs.getString("actual_password"),
					srs.getInt("actual_timeout_sec")
					);
			
			sc.actualQuery.commandText = srs.getString("actual_command");
						
			sc.expectedConnection = DataUtils.getConnectionFromDriver(
					srs.getString("expected_jdbc_driver"), 
					srs.getString("expected_jdbc_url"), 
					srs.getString("expected_username"), 
					srs.getString("expected_password"),
					srs.getInt("expected_timeout_sec")
					);
			
			
			sc.expectedQuery.commandText = srs.getString("expected_command");
			
			sc.allowedCaseFailureRate = srs.getDouble("allowed_case_failure_rate");
			sc.caseFailureRecordLimit = srs.getInt("case_failure_record_limit");
			sc.caseSuccessRecordLimit = srs.getInt("case_success_record_limit");
			sc.modulus = srs.getInt("modulus");
			sc.modularity= srs.getInt("modularity");
			
			sc.Tags = DataUtils.getListFromString(srs.getString("tag_list"));

			
			// populate the grains
			List<String> grainList = DataUtils.getListFromString(srs.getString("grain_list"));
			
			for(String columnName:grainList)
			{
				sc.BaseTestCase.Grains.add(new Grain(columnName));	
			}
			

			// populate measures not already declared in database
			scenarioFilter = new RowsetStringFilter("scenario_uid", new String[]{sc.identifier});
			
			frs.beforeFirst();
			frs.setFilter(scenarioFilter);
			
			while(frs.next())
			{
				
				m = new Measure(frs.getString("measure_name"));
				
				m.precision = frs.getInt("precision");
				m.allowedVariance = frs.getDouble("allowed_variance");
				m.allowedVarianceRate = frs.getDouble("allowed_variance_rate");
				
				sc.BaseTestCase.Measures.add(m);
			}
						
			harness.addScenario(sc);			
		}
		
		DataUtils.disposeResulset(srs);
		
	}
	
	public void writeHarness(Harness harness) throws Exception
	{
		for(Scenario sc : harness.Scenarios)
		{		
			// only write executed tests
			if(sc.testGuid != null)
			{
				this.writeHarnessTest(sc);
								
			}
		}
	}
	
	public void writeHarnessTest(Scenario sc) throws Exception
	{
		String commandText;
		String parameterizedCommandText;
		
		commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.INSERT_TEST_RESOURCE);


		Application.logger.info("Writing test " + sc.identifier);
		
		// prepare calculated parameters
		int failureCount = sc.getFailureCount();
		int successCount = sc.getSuccessCount();
		
		String errorFlag;
		String failureFlag;
		
		// determine the error flag
		if(sc.testException != null || sc.actualQuery.queryException != null || sc.expectedQuery.queryException != null)
		{
			// any exceptions flags an error
			errorFlag = "Y";
		} else
		{
			errorFlag = "N";
		}
		
		// determine the failure flag
		if(errorFlag == "Y")
		{
			// tests with exceptions do not count as failures
			failureFlag = "Y";
			
		} else if (failureCount == 0)
		{			
			// no case results automatically succeeds
			failureFlag = "N";
			
		} else if ((failureCount * 1.00/(failureCount + successCount)) > sc.allowedCaseFailureRate)
		{
			// exceeds allowed failure rate
			failureFlag = "Y";
			
		} else
		{
			failureFlag = "N";
		}
		
		
		/*  COLUMN LIST
		 
		  [test_uid]
		, [scenario_uid]
		, [test_dtm]
		
		, [modularity]
		, [modulus]
		
		, [failure_case_ct]
		, [success_case_ct]
		. [case_failure_rate]
		, [allowed_case_failure_rate]
		, [failure_flag]
		
		, [error_flag]
		, [test_error_message]
		, [expected_error_message]
		, [actual_error_message]

		*/
		
		Object[] parameters = {
				
		  DataUtils.delimitSQLString(sc.testGuid),
		  DataUtils.delimitSQLString(sc.identifier),
		  DataUtils.delimitSQLString(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(sc.testDate)),
		  
		  String.valueOf(sc.modularity),
		  String.valueOf(sc.modulus),
		  
		  String.valueOf(failureCount),
		  String.valueOf(successCount),
		  String.valueOf(sc.getCaseFailureRate()),
		  String.valueOf(sc.allowedCaseFailureRate),
		  DataUtils.delimitSQLString(failureFlag),

		  DataUtils.delimitSQLString(errorFlag),
		  (sc.testException == null) ? "NULL" : DataUtils.delimitSQLString(Application.getExceptionStackTrace(sc.testException, 2000)),
		  (sc.expectedQuery.queryException == null) ? "NULL" : DataUtils.delimitSQLString(Application.getExceptionStackTrace(sc.expectedQuery.queryException, 2000)),
		  (sc.actualQuery.queryException == null) ? "NULL" : DataUtils.delimitSQLString(Application.getExceptionStackTrace(sc.actualQuery.queryException,2000))
		  
		};
		
		// apply the values to the command text
		MessageFormat mf = new MessageFormat(commandText);
		
		parameterizedCommandText = mf.format(parameters);
				
		// execute the command
		DataUtils.executeCommand(parameterizedCommandText, this);
		
		// write the test cases		
		
		int successAllowance = sc.caseSuccessRecordLimit;
		int failureAllowance = sc.caseFailureRecordLimit;
		TestCase tc;
		
		for(String key: sc.TestCases.keySet())
		{
			tc = sc.TestCases.get(key);
			
			// write the test case
			this.writeHarnessTestCase(sc, tc, failureAllowance, successAllowance);
			
			// update counters
			failureAllowance -= tc.failureCount();
			successAllowance -= tc.successCount();
									
			// if both failure and success record limits are exceeded...
			if(failureAllowance <= 0 && successAllowance <= 0)
			{
				// exit the test case iteration
				break;
			}
		}
		
	}
	
	public void writeHarnessTestCase(Scenario sc, TestCase tc, int failureAllowance, int successAllowance) throws Exception
	{
		String commandText;
		String parameterizedCommandText;
		
		commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.INSERT_TEST_CASE_RESOURCE);
	
		int maxGrainIndex = tc.Grains.size() - 1;
		
		String grain01Name = (maxGrainIndex >= 0) ? tc.Grains.get(0).columnName: null;
		String grain01Text = (maxGrainIndex >= 0) ? tc.Grains.get(0).valueToString(): null;
		String grain02Name = (maxGrainIndex >= 1) ? tc.Grains.get(1).columnName: null;
		String grain02Text = (maxGrainIndex >= 1) ? tc.Grains.get(1).valueToString(): null;
		String grain03Name = (maxGrainIndex >= 2) ? tc.Grains.get(2).columnName: null;
		String grain03Text = (maxGrainIndex >= 2) ? tc.Grains.get(2).valueToString(): null;
		String grain04Name = (maxGrainIndex >= 3) ? tc.Grains.get(3).columnName: null;
		String grain04Text = (maxGrainIndex >= 3) ? tc.Grains.get(3).valueToString(): null;
		String grain05Name = (maxGrainIndex >= 4) ? tc.Grains.get(4).columnName: null;
		String grain05Text = (maxGrainIndex >= 4) ? tc.Grains.get(4).valueToString(): null;
		
		String failureFlag;

		for(Measure m : tc.Measures)
		{	
			
			// abort writing the test case if both allowances are used
			if(failureAllowance <= 0 && successAllowance <= 0)
			{
				return;
			}
						
			// determine if the measure is failed
			if(m.isFailed() == true)
			{
				
				// if there is no remaining failure allowance...
				if(failureAllowance <= 0)
				{
					// iterate to the next measure
					continue;
				}
				
				// set the failure flag
				failureFlag = "Y";		
				
				// decrement the failure allowance by 1
				failureAllowance--;
				
			} else 
			{
				// if there is no remaining success allowance...
				if(successAllowance <= 0)
				{
					// iterate to the next measure
					continue;
				}
				
				// set the failure flag
				failureFlag = "N";		
				
				// decrement the success allowance by 1
				successAllowance--;
				
			}
			
			/*  COLUMN LIST
			 
			  [test_uid]
			, [measure_name]
			, [precision]
			, [expected_value]
			, [expected_text]
			, [actual_value]
			, [actual_text]
			, [result_variance]
			, [result_variance_rate]
			, [allowed_variance]
			, [allowed_variance_rate]
			, [failure_flag]
			, [grain_hash]
			, [grain_01_name]
			, [grain_01_text]
			, [grain_02_name]
			, [grain_02_text]
			, [grain_03_name]
			, [grain_03_text]
			, [grain_04_name]
			, [grain_04_text]
			, [grain_05_name]
			, [grain_05_text]
			
			*/
			
			Object[] parameters = {
					
			  DataUtils.delimitSQLString(sc.testGuid),
			  DataUtils.delimitSQLString(m.columnName),
			  String.valueOf(m.precision),
			  
			  (m.isNumeric() ? m.expectedValueToString() : "NULL"),
			  DataUtils.delimitSQLString((!m.isNumeric() ? m.expectedValueToString() : null)),
			  
			  (m.isNumeric() ? m.actualValueToString() : "NULL"),
			  DataUtils.delimitSQLString((!m.isNumeric() ? m.actualValueToString() : null)),
			  
			  String.valueOf(m.getVariance()),
			  String.valueOf(m.getVarianceRate()),
			  
			  String.valueOf(m.allowedVariance),
			  String.valueOf(m.allowedVarianceRate),
			  
			  DataUtils.delimitSQLString(failureFlag),
			  
			  DataUtils.delimitSQLString(tc.hashKey),
			  
			  DataUtils.delimitSQLString(grain01Name),
			  DataUtils.delimitSQLString(grain01Text),
			  
			  DataUtils.delimitSQLString(grain02Name),
			  DataUtils.delimitSQLString(grain02Text),

			  DataUtils.delimitSQLString(grain03Name),
			  DataUtils.delimitSQLString(grain03Text),
			  
			  DataUtils.delimitSQLString(grain04Name),
			  DataUtils.delimitSQLString(grain04Text),
			  
			  DataUtils.delimitSQLString(grain05Name),
			  DataUtils.delimitSQLString(grain05Text),
			  
			};
			
			// apply the values to the command text
			MessageFormat mf = new MessageFormat(commandText);
			
			parameterizedCommandText = mf.format(parameters);
					
			// execute the command
			DataUtils.executeCommand(parameterizedCommandText, this);
		
		}
	}
	
	public void bootstrap() throws FileNotFoundException, IOException, SQLException
	{
		String commandText;
		ResultSet rs;
		
		// execute the test for existing tables
		commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.BOOTSTRAP_VALIDATE_RESOURCE);		
		rs = DataUtils.executeCommandWithResult(commandText, this);
		
		int rowCount = 0;
		
		boolean result = rs.next();
		if(result == true)
		{
			rowCount = rs.getInt(1);
		}
		
		// dispose of resultset
		DataUtils.disposeResulset(rs);

		
		// if not records are returned, the conduct bootstrap
		if(rowCount == 0)
		{
			commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.BOOTSTRAP_RESOURCE);		
			DataUtils.executeCommand(commandText, this);			
		}
		
	}

}
