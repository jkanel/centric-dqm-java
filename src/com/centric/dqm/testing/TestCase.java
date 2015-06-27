package com.centric.dqm.testing;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestCase {
	
	public final static String GRAIN_VALUE_DELIMITER = "";
	
	public final static int INTERNAL_DATA_TYPE_INTEGER = 1;	
	public final static int INTERNAL_DATA_TYPE_NUMERIC = 2;
	public final static int INTERNAL_DATA_TYPE_DATE = 3;
	public final static int INTERNAL_DATA_TYPE_TEXT = 4;
	
	public final static int SCENARIO_MODE_ACTUAL = 1;
	public final static int SCENARIO_MODE_EXPECTED = 2;
		
	public List<Grain> Grains = new ArrayList<Grain>();
	public List<Measure> Measures = new ArrayList<Measure>();
	
	protected int _successCount = -1;
	protected int _failureCount = -1;
	
	protected void updateCounts()
	{

		this._successCount = 0;
		this._failureCount = 0;
		
		for(Measure m : this.Measures)
		{
			this._successCount += m.successCount();
			this._failureCount += m.failureCount();
		}
	}
	
	
	public int successCount()
	{
		if(this._successCount == -1)
		{
			updateCounts();
		}
		
		return this._successCount;
	}
	
	public int failureCount()
	{
		if(this._failureCount == -1)
		{
			updateCounts();
		}
		
		return this._failureCount;
	}
	
	public void loadResultset(ResultSet rs, int scenarioMode) throws SQLException
	{
		String columnName;
		
		ResultSetMetaData md = rs.getMetaData();
		
		// iterate through each column
		for(int col = 1; col <= md.getColumnCount() ; col++)
		{
		
			columnName = md.getColumnLabel(col);
			
			for(Grain g: this.Grains)
			{
				if(g.columnName.equals(columnName))
				{
								
					if(g.internalDataType == (TestCase.INTERNAL_DATA_TYPE_NUMERIC))
					{
						g.valueNumeric = rs.getDouble(col);
					}
					else if(g.internalDataType == (TestCase.INTERNAL_DATA_TYPE_INTEGER))
					{
						g.valueInt = rs.getInt(col);
					}
					else if(g.internalDataType == (TestCase.INTERNAL_DATA_TYPE_DATE))
					{
						g.valueDate = rs.getDate(col);
					}
					else if(g.internalDataType == (TestCase.INTERNAL_DATA_TYPE_TEXT))
					{
						g.valueText = rs.getString(col);
					}
					
					break;
				}
			}
			
			for(Measure m: this.Measures)
			{
				if(m.columnName.equals(columnName))
				{
					
				
					if(m.internalDataType == (TestCase.INTERNAL_DATA_TYPE_NUMERIC))
					{
						switch(scenarioMode)
						{
						case SCENARIO_MODE_ACTUAL:
							m.actualValueNumeric = rs.getDouble(col);
							break;
						case SCENARIO_MODE_EXPECTED:
							m.expectedValueNumeric = rs.getDouble(col);
							break;					
						}					
					}
					
					else if(m.internalDataType == (TestCase.INTERNAL_DATA_TYPE_INTEGER))
					{
						switch(scenarioMode)
						{
						case SCENARIO_MODE_ACTUAL:
							m.actualValueInt = rs.getInt(col);
						case SCENARIO_MODE_EXPECTED:
							m.expectedValueInt = rs.getInt(col);
							break;					
						}
					}
					
					else if(m.internalDataType == (TestCase.INTERNAL_DATA_TYPE_DATE))
					{
						switch(scenarioMode)
						{
						case SCENARIO_MODE_ACTUAL:
							m.actualValueDate = rs.getDate(col);
							break;
						case SCENARIO_MODE_EXPECTED:
							m.expectedValueDate = rs.getDate(col);
							break;					
						}
					}
					
					else if(m.internalDataType == (TestCase.INTERNAL_DATA_TYPE_TEXT))
					{
						switch(scenarioMode)
						{
						case SCENARIO_MODE_ACTUAL:
							m.actualValueText =  rs.getString(col);
							break;
						case SCENARIO_MODE_EXPECTED:
							m.expectedValueText =  rs.getString(col);
							break;					
						}			
					}
					
					break;
					
				}
			}
		}		
	}
	
	/**
	 * Returns an internal Comparison object data type value (String)
	 * derived from a java.sql.Types value.
	 * @param type
	 * @return
	 * @throws Exception 
	 */
	public static int getComparisonDataType(int type) throws Exception
	{
		switch(type)
		{
		case java.sql.Types.INTEGER : 
		case java.sql.Types.BIGINT: 
		case java.sql.Types.SMALLINT: 
		case java.sql.Types.TINYINT:
		case java.sql.Types.BIT:
		
			return TestCase.INTERNAL_DATA_TYPE_INTEGER;
		
		case java.sql.Types.DECIMAL: 
		case java.sql.Types.FLOAT: 
		case java.sql.Types.DOUBLE: 
		case java.sql.Types.NUMERIC: 
		
			return TestCase.INTERNAL_DATA_TYPE_NUMERIC;
			
		case java.sql.Types.DATE: 
		case java.sql.Types.TIME: 
		case java.sql.Types.TIMESTAMP:
			
			return TestCase.INTERNAL_DATA_TYPE_INTEGER;
			
		case java.sql.Types.VARCHAR: 
		case java.sql.Types.CHAR: 
		case java.sql.Types.NVARCHAR: 
		case java.sql.Types.NCHAR: 
			
			return TestCase.INTERNAL_DATA_TYPE_TEXT;
			
		default:
				
			throw new Exception("Unrecognized internal data type. Java.sql.Types = " + Integer.toString(type));
		}
	}
	
	public TestCase cloneDefinition()
	{
		TestCase tc = new TestCase();
		
		for(Grain g : this.Grains)
		{
			tc.Grains.add(g.cloneDefinition());
		}
		
		for(Measure m: this.Measures)
		{
			tc.Measures.add(m.cloneDefinition());
		}
		
		return tc;
	}
	
	/**
	 * Builds the key for use in a hash map.
	 * @return Returns the hash key string value.
	 * @throws SQLException 
	 */
	public static String generateHashKey(List<Grain> grains, ResultSet rs) throws SQLException
	{
		String key = "";
		
		for(int n = 0; n < grains.size();  n++)
		{
			key += rs.getString(grains.get(n).columnName);	
		}
		
		return key;
		
	}

	

}

