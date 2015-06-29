package com.centric.dqm.testing;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.centric.dqm.Application;
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

	public Map<String, TestCase> TestCases = new HashMap<String, TestCase>();
	
	public String identifier;
	public String description;
	
	public String testGuid = null;
	public Date testDate = null;
	
	protected int _successCount = -1;
	protected int _failureCount = -1;
	
	public List<String> Tags = new ArrayList<String>();
	
	public String getTestGuid() {
		return testGuid;
	}

	public void performTest()
	{
		
		// assign a test guid
		this.testGuid = java.util.UUID.randomUUID().toString();
		
		// assign the execution time
		this.testDate = new Date();
		
		Application.logger.info("Testing scenario \"" + this.identifier + "\" (" + this.testGuid + ").");
		
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
			ers = expectedQuery.execute(expectedConnection, this.modulus, this.modularity);
			
			// verify expected metadata
			// the expected query cannot introduce new measure columns
			this.verifyMetaData(ers.getMetaData(), true);
			
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
			ars = actualQuery.execute(actualConnection, this.modulus, this.modularity);
			
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
		
		ars = null;



	}
	
	/**
	 * Ensures all grain and measure in the base comparison
	 * also exist in this comparison object instance.
	 * @param BaseTestCase The comparison used as a reference.
	 * @throws Exception 
	 */
	public void verifyMetaData(ResultSetMetaData md, boolean isExpected) throws Exception
	{
		String columnName;
		int mdInternalDataType;

		// validate that the grain column name exists in the metadata
		// this is a requirement for both expected and actual queries
		for(Grain g : this.BaseTestCase.Grains)
		{
			if(this.existsMetaDataColumn(g.columnName, md) == false)
			{
				throw new IllegalStateException("The specified query does not contain a required grain column, \"" 
						+ g.columnName + "\".");
			}
			
			mdInternalDataType = TestCase.getComparisonDataType(this.getMetaDataColumnType(g.columnName, md));
			
			// set the data type if unknown
			if(g.internalDataType == TestCase.INTERNAL_DATA_TYPE_UNKNOWN)
			{
				g.internalDataType = mdInternalDataType;
				
			// if metadata data type is different than the previously set
			// data type, throw an exception
			} else if (g.internalDataType != mdInternalDataType)
			{
				throw new IllegalStateException("The data type of the grain column, \"" 
						+ g.columnName + "\" is not compatible across queries.");
			}
			
		}
		
		
		// add new measure columns if warranted
		for(int n = 1; n <= md.getColumnCount(); n++)
		{
			
			columnName = md.getColumnLabel(n);			
			
			// get the internal data type indicated by metadata
			mdInternalDataType = TestCase.getComparisonDataType(md.getColumnType(n));
			
			// get the existing measure				
			Measure m = this.getMeasureByColumn(columnName);
			
			// validate the existing measure data type for the actual metadata
			if(isExpected == false)
			{

				// if the measure exist; and data types are different 
				// than metadata, throw an exception
				if(m != null && m.internalDataType != mdInternalDataType)
				{
					throw new IllegalStateException("The data type of the comparison column, \"" 
							+ columnName + "\" is not compatible across queries.");
				}
				
			// enrich and add measures for expected metadata
			} else
			{
				
				// if the measure exists 
				if(m != null)
				{
					// set the internal data type
					m.internalDataType = mdInternalDataType;
					
				// if the measure is new (does not exist)
				} else
				{				
					// preparing to add a new measure
					// if the column is not a grain column
					if(this.existsGrainColumn(columnName) == false)
					{
	
						Application.logger.info("Adding \"" + this.identifier + "\" metadata \"" + columnName + "\".");
						
						// add to the Measures
						Measure newMeasure = new Measure(columnName);
						newMeasure.internalDataType = mdInternalDataType;
						
						this.BaseTestCase.Measures.add(newMeasure);
											
						// force add to the existing test case measures
						// this code should not generally be exercised
						// if expected metadata is treated prior to actual metadata
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
		TestCase tc = null;
		
		while (rs.next()) {
			
			// generate the key
			String hashKey = TestCase.generateHashKey(this.BaseTestCase.Grains, rs);
			
			tc = this.TestCases.get((String)hashKey);
			
			// if the test case does not exist
			if(tc == null)
			{
				// clone the base test case
				tc = this.BaseTestCase.cloneDefinition();
				
				// assign the hash key
				tc.hashKey = hashKey;
				
				// add to the test cases
				TestCases.put(hashKey, tc);
				
				// populate test case grain values
				for(Grain g: tc.Grains)
				{
					g.assignValue(rs);
				}
				
	        // otherwise retrieve the existing test case
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
	
	protected void updateCounts()
	{

		this._successCount = 0;
		this._failureCount = 0;
		
		for(String key : this.TestCases.keySet())
		{
			this._successCount += this.TestCases.get(key).successCount();
			this._failureCount += this.TestCases.get(key).failureCount();
		}
	}
	
	
	public int getSuccessCount()
	{
		if(this._successCount == -1)
		{
			updateCounts();
		}
		
		return this._successCount;
	}
	
	public int getFailureCount()
	{
		if(this._failureCount == -1)
		{
			updateCounts();
		}
		
		return this._failureCount;
	}
	
	public Double getCaseFailureRate()
	{
		
		int failureCount = this.getFailureCount();
		int totalCount = failureCount + this.getSuccessCount();
		
		if(totalCount == 0)
		{
			return 0.0d;
			
		} else
		{
			Double x =  (double)failureCount *  1.000 / (double)totalCount;
			return x;
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
	
	Grain getGrainByColumn(String columnName)
	{
		for(Grain m : this.BaseTestCase.Grains)			
		{
			if(m.columnName.equals(columnName))
			{
				return m;
			}			
		}
		
		return null;		
	}
	
	
	
	Measure getMeasureByColumn(String columnName)
	{
		for(Measure m : this.BaseTestCase.Measures)			
		{
			if(m.columnName.equals(columnName))
			{
				return m;
			}			
		}
		
		return null;		
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
	
	int getMetaDataColumnType(String columnName, ResultSetMetaData md) throws SQLException
	{
		int columnIndex;
		
		for(columnIndex = 1; columnIndex <= md.getColumnCount(); columnIndex++)
		{
			if(md.getColumnLabel(columnIndex).equals(columnName))
			{
				return md.getColumnType(columnIndex);
			}
		}
		
		return -1;
	}	

	String getKeyFromTestCases(String value)
	{
		for(String key : this.TestCases.keySet())
		{
			if (key.equalsIgnoreCase(value))
			{
				return key;
			}
		}
		
		return null;
	}
	
}
