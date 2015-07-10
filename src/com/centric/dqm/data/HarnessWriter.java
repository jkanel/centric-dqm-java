package com.centric.dqm.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;

import com.centric.dqm.Application;
import com.centric.dqm.testing.Harness;
import com.centric.dqm.testing.Measure;
import com.centric.dqm.testing.Scenario;
import com.centric.dqm.testing.TestCase;

public class HarnessWriter {

	
	public static void writeHarness(IConnection con, Harness harness) throws Exception
	{
		for(Scenario sc : harness.Scenarios)
		{		
			// only write executed tests
			if(sc.testGuid != null)
			{
				HarnessWriter.writeTest(con, sc);
								
			}
		}
	}
	
	public static void deleteTestCase(IConnection con, Integer purgeDays) throws Exception
	{
		String commandText;
		String parameterizedCommandText;
		
		// only proceed if the purgeDays specified is in range
		if(purgeDays == null || purgeDays < 0)
		{
			return;
		}
		
		Application.logger.info("Purging test cases prior to " + String.valueOf(purgeDays) + " days ago");
		
		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.DELETE_TEST_CASE_RESOURCE);
		
		Object[] parameters = {				
				DataUtils.delimitSQLString(String.valueOf(purgeDays)),
		};
		
		// apply the values to the command text
		MessageFormat mf = new MessageFormat(commandText);
		
		parameterizedCommandText = mf.format(parameters);
				
		// execute the command
		// allow parsing
		DataUtils.executeCommand(parameterizedCommandText, con);		
		
	}
	
	public static void writeTest(IConnection con, Scenario sc) throws Exception
	{
		
		String commandText;
		String parameterizedCommandText;
		
		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.INSERT_TEST_RESOURCE);
	
	
		Application.logger.info("Writing test " + sc.identifier + " results");
		
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
		  (sc.testException == null) ? "NULL" : DataUtils.delimitSQLString(sc.testException.getMessage()),
		  (sc.expectedQuery.queryException == null) ? "NULL" : DataUtils.delimitSQLString(sc.expectedQuery.queryException.getMessage()),
		  (sc.actualQuery.queryException == null) ? "NULL" : DataUtils.delimitSQLString(sc.actualQuery.queryException.getMessage())
		  
		};
		
		// apply the values to the command text
		MessageFormat mf = new MessageFormat(commandText);
		
		parameterizedCommandText = mf.format(parameters);
				
		// execute the command
		DataUtils.executeCommand(parameterizedCommandText, con);
		
		// write the test cases		
		
		int successAllowance = sc.caseSuccessRecordLimit;
		int failureAllowance = sc.caseFailureRecordLimit;
		TestCase tc;
		
		for(String key: sc.TestCases.keySet())
		{
			tc = sc.TestCases.get(key);
			
			// write the test case
			HarnessWriter.writeTestCase(con, sc, tc, failureAllowance, successAllowance);
			
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

	protected static void writeTestCase(IConnection con, Scenario sc, TestCase tc, int failureAllowance, int successAllowance) throws Exception
	{
		String commandText;
		String parameterizedCommandText;
		
		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.INSERT_TEST_CASE_RESOURCE);
	
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
			DataUtils.executeCommand(parameterizedCommandText, con);
		
		}
	}
	
		
	public static void updateQueryCommand(IConnection con, Scenario sc, String scenarioMode, String commandText) throws FileNotFoundException, IOException
	{
		
		String resourceText = null;		
		resourceText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.UPDATE_SCENARIO_QUERY);		
		
		Object[] parameters = {				
		  DataUtils.delimitSQLString(sc.identifier),
		  scenarioMode,
		  DataUtils.delimitSQLString(commandText)
		};
		
		// apply the values to the command text
		MessageFormat mf = new MessageFormat(resourceText);		
		String parameterizedCommandText = mf.format(parameters);
				
		// execute the command
		// Suppress parsing due to unknown contents of commandText 
		DataUtils.executeCommand(parameterizedCommandText, con, null);
		
		
	}

}
