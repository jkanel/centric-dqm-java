package com.centric.dqm.data;

import java.sql.ResultSet;
import java.util.Date;
import java.util.List;

import javax.sql.rowset.FilteredRowSet;

import com.centric.dqm.testing.Grain;
import com.centric.dqm.testing.Harness;
import com.centric.dqm.testing.Measure;
import com.centric.dqm.testing.Scenario;
import com.sun.rowset.FilteredRowSetImpl;

public class HarnessReader {
	
	public static Date getDate(IConnection con) throws Exception
	{
		
		String commandText;
		ResultSet rs = null;
		Date currentDate = null;
		
		// execute the test for existing tables
		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.SELECT_CURRENT_DATE_RESOURCE);	
		rs = DataUtils.executeCommandWithResult(commandText, con);
		
		while(rs.next())
		{
			currentDate = rs.getDate("current_dt");
			break;
		}
		
		DataUtils.disposeResulset(rs);

		return currentDate;
		
	}
	
	public static void readHarness(IConnection con, Harness harness) throws Exception
	{
		
		String commandText;
		ResultSet srs = null;
		ResultSet smrs = null;
		Scenario sc = null;
		RowsetStringFilter scenarioFilter;
		Measure m;
		
		// execute the test for existing tables
		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.SELECT_SCENARIOS_RESOURCE);	
		srs = DataUtils.executeCommandWithResult(commandText, con);
		
		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.SELECT_SCENARIO_MEASURES_RESOURCE);	
		smrs = DataUtils.executeCommandWithResult(commandText, con);
		
		// push the measure scenarios into a filtered rowset
		FilteredRowSet frs = new FilteredRowSetImpl();
		frs.populate(smrs);
		
		DataUtils.disposeResulset(smrs);

		while(srs.next())
		{
			sc = new Scenario();
			
			sc.identifier = srs.getString("scenario_uid");		
			
			sc.actualConnection = DataUtils.getConnection(
					srs.getString("actual_jdbc_driver"), 
					srs.getString("actual_jdbc_url"), 
					srs.getString("actual_username"), 
					srs.getString("actual_password"),
					srs.getInt("actual_timeout_sec")
					);
			
			sc.actualQuery.commandText = srs.getString("actual_command");
						
			sc.expectedConnection = DataUtils.getConnection(
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
			
			String fneChar = srs.getString("flexible_null_equality_flag");
			sc.flexibleNullEqualityFlag = (fneChar == null ? false : fneChar.equals("Y"));
			
			String activeChar = srs.getString("active_flag");
			sc.activeFlag = (activeChar == null ? false : activeChar.equals("Y"));
			
			
			sc.Tags = DataUtils.getListFromLowerCaseString(srs.getString("tag_list"));

			
			// populate the grains
			// strip out left and right brackets
			List<String> grainList = DataUtils.getListFromString(srs.getString("grain_list"),",", "[]");
			
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
				
				// default to the scenario null equals zero setting
				if(srs.getString("flexible_null_equality_flag") != null)
				{
					m.flexibleNullEqualityFlag = frs.getString("flexible_null_equality_flag").equals("Y");
				} else 
				{
					m.flexibleNullEqualityFlag = sc.flexibleNullEqualityFlag;
				}
				
				m.precision = frs.getInt("precision");
				m.allowedVariance = frs.getDouble("allowed_variance");
				m.allowedVarianceRate = frs.getDouble("allowed_variance_rate");
				
				sc.BaseTestCase.Measures.add(m);
			}
						
			harness.addScenario(sc);			
		}
		
		DataUtils.disposeResulset(srs);
		
	}

}
