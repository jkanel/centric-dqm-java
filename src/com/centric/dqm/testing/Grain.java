package com.centric.dqm.testing;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Grain {
		
	public String columnName;
	
	public int internalDataType;
	
	public Double valueNumeric;
	public String valueText;
	public Date valueDate;
	public Integer valueInt;
	
	protected boolean _isValueAssigned = false;
	
	public Grain cloneDefinition()
	{
		Grain g2 = new Grain();
		g2.internalDataType = this.internalDataType;
		g2.columnName = this.columnName;		
		return g2;		
	}
	
	public void assignValue(ResultSet rs) throws SQLException
	{
		
		if(this._isValueAssigned == true)
		{
			throw new IllegalStateException("The actual value for " + this.columnName 
					+ " was already assigned.  This may indicate multiple records having the same grain.");
		}
		
		switch(this.internalDataType)
		{	
			case TestCase.INTERNAL_DATA_TYPE_NUMERIC:
			
				this.valueNumeric = rs.getDouble(this.columnName);
			
			case TestCase.INTERNAL_DATA_TYPE_INTEGER:
			
				this.valueInt = rs.getInt(this.columnName);
			
			case TestCase.INTERNAL_DATA_TYPE_DATE:
			
				this.valueDate = rs.getDate(this.columnName);
			
			case TestCase.INTERNAL_DATA_TYPE_TEXT:
			
				this.valueText = rs.getString(columnName);		
		}
		
		this._isValueAssigned = true;
		
	}
	
	public String valueToString()
	{
		
		switch(this.internalDataType)
		{
		
		case TestCase.INTERNAL_DATA_TYPE_NUMERIC:
		
			return this.valueNumeric.toString();
		
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			return this.valueInt.toString();
		
		case TestCase.INTERNAL_DATA_TYPE_DATE:
		
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
			return dateFormatter.format(this.valueDate);
		
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			return this.valueText;
		default:
			
			return (String)null;
			
		}
		
	}
	
}
