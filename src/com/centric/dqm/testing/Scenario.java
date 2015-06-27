package com.centric.dqm.testing;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.IConnection;

public class Scenario {
	
	public static final int MAX_GRAIN_COUNT = 5;
	
	public TestCase BaseTestCase = new TestCase();
	
	public Query actualQuery = new Query();
	public Query expectedQuery = new Query();
	
	public IConnection actualConnection;
	public IConnection expectedConnection;
	
	public Exception testException;
	boolean testFailureFlag = false;
	
	public Integer modulus = null;
	public Integer modularity = null;
	public Integer caseFailureRecordLimit = null;
	public Integer caseSuccessRecordLimit = null;
	public Double allowedCaseFailureRate = null;

	Map<String, TestCase> TestCases = new HashMap<String, TestCase>();
	
	public String identifier;
	public String description;
	
	private String testGuid = null;
	
	public List<String> Tags = new ArrayList<String>();
	
	public String getTestGuid() {
		return testGuid;
	}

	public void performTest()
	{
		
		// assign a test guid
		this.testGuid = java.util.UUID.randomUUID().toString();
		
		
		// verify that grain columns are less than the maximum allowable
		if(this.BaseTestCase.Grains.size() > Scenario.MAX_GRAIN_COUNT)
		{
			this.testException = new IllegalStateException("The maximum number of grain columns supported is " 
					+ String.valueOf(Scenario.MAX_GRAIN_COUNT) + ".  The number of grain columns specified is " 
					+ String.valueOf(this.BaseTestCase.Grains.size()) + ".");
			
			return;
		}
		
		// process the expected query
		ResultSet ers = null;
				
		try {
			
			// run the expected query
			ers = expectedQuery.execute(expectedConnection);
			
			// verify expected metadata
			// the expected query cannot introduce new measure columns
			this.verifyMetaData(ers.getMetaData(), false);
			
			// load expected query results		
			this.loadComparisons(ers, true);
			
			
		} catch (Exception e) {
			
			this.testException = e;
			testFailureFlag = true;
			
		} finally
		{
			// dispose of the resultset
			DataUtils.disposeResulset(ers);			
		}

		
		// process the actual query
		ResultSet ars =  null;
		
		try {
			
			// run the actual query
			ars = actualQuery.execute(actualConnection);
			
			// verify actual metadata
			// the actual query cannot introduce new measure columns
			this.verifyMetaData(ars.getMetaData(), false);
			
			// load actual query results		
			this.loadComparisons(ars, false);
			
			
		} catch (Exception e) {
			
			this.testException = e;
			testFailureFlag = true;
			
		} finally
		{
			// dispose of the resultset
			DataUtils.disposeResulset(ars);			
		}



	}
	
	/**
	 * Ensures all grain and measure in the base comparison
	 * also exist in this comparison object instance.
	 * @param BaseTestCase The comparison used as a reference.
	 * @throws SQLException 
	 * @throws Exception 
	 */
	public void verifyMetaData(ResultSetMetaData md, boolean allowNewMeasures) throws SQLException
	{
		String columnName;


		// validate that the grain column name exists in the metadata
		// this is a requirement for both expected and actual queries
		for(Grain g : this.BaseTestCase.Grains)
		{
			if(this.existsMetaDataColumn(g.columnName, md) == false)
			{
				throw new IllegalStateException("The specified query does not contain a required grain column, \"" + g.columnName + "\".");
			}
		}
		
		
		// add new measure columns if warranted
		if(allowNewMeasures == true)
		{
			
			for(int n = 1; n <= md.getColumnCount(); n++)
			{
				// capture the column name to find
				columnName = md.getColumnLabel(n);
				
				if(this.existsMeasureColumn(columnName) == false)		
				{				
					// verify that the column is not a grain column
					if(this.existsGrainColumn(columnName) == false)
					{
	
						// add to the Measures
						Measure newMeasure = new Measure(columnName);
						this.BaseTestCase.Measures.add(newMeasure);
											
						// force add to the existing Comparison measures
						for(String key: this.TestCases.keySet())
						{
							TestCases.get(key).Measures.add(newMeasure.cloneDefinition());
						}										
					}
				}		
			}
		}
		
	}
	
	/**
	 * Populates grain and measure values from a resultset.
	 * @param rs Resultset from which values are populated; positioned on the relevant record. 
	 * @param isExpected Indicates that the resultset is from an expected or actual query.
	 */
	public void loadComparisons(ResultSet rs, boolean isExpected) throws SQLException
	{
		//loop through the resulset : for each...	
		String key;
		TestCase tc = null;
		
		while (rs.next()) {
			
			// generate the key
			key = TestCase.generateHashKey(this.BaseTestCase.Grains, rs);
			
			// find existing tests case based on grain values;
			// or create a new test case
			if(TestCases.containsKey(key)==false)
			{
				tc = this.BaseTestCase.cloneDefinition();
				TestCases.put(key, tc);
				
				// populate grain values
				for(Grain g: tc.Grains)
				{
					g.assignValue(rs);
				}
			} 
			else
			{
				tc = this.TestCases.get(key);
			}
			
			// populate each measure column
			for(Measure m: tc.Measures)
			{
				if(isExpected == true)
				{
					m.assignActualValue(rs);
				}
				else
				{
					m.assignExpectedValue(rs);
				}		
			}
	    }
	}
	
	/**
	 * Determine if a grain in the Grains list has a column matching the parameter name.
	 * @param columnName Name of the column to check.
	 * @return  True if the column name exists.
	 */
	boolean existsGrainColumn(String columnName)
	{
		for(Grain g : this.BaseTestCase.Grains)			
		{
			if(g.columnName.equals(columnName))
			{
				return true;
			}			
		}
		
		return false;		
	}
	
	/**
	 * Determine if a measure in the Measures list has a column matching the parameter name.
	 * @param columnName Name of the column to check.
	 * @return  True if the column name exists.
	 */		
	boolean existsMeasureColumn(String columnName)
	{
		for(Measure m : this.BaseTestCase.Measures)			
		{
			if(m.columnName.equals(columnName))
			{
				return true;
			}			
		}
		
		return false;		
	}

	
	/**
	 * Determine if a measure in the Measures list has a column matching the parameter name.
	 * @param columnName Name of the column to check.
	 * @return  True if the column name exists.
	 */		
	boolean existsTag(String tag)
	{
		for(String t : this.Tags)			
		{
			if(t.equals(tag))
			{
				return true;
			}			
		}
		
		return false;		
	}

	/**
	 * Determine a column exists in a resultset's meta data
	 * @param columnName Name of the column to check.
	 * @param md Metadata to be searched for a matching column name.
	 * @return  True if the column name exists.
	 * @throws SQLException 
	 */		
	boolean existsMetaDataColumn(String columnName, ResultSetMetaData md) throws SQLException
	{
		int columnIndex;
		
		for(columnIndex = 1; columnIndex <= md.getColumnCount(); columnIndex++)
		{
			if(md.getColumnLabel(columnIndex).equals(columnName))
			{
				return true;
			}
		}
		
		return false;
	}	

}
