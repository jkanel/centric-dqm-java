package com.centric.dqm.testing;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Grain {
		
	public String columnName;
	
	public int internalDataType;
	
	public Double valueNumeric;
	public String valueText;
	public Date valueDate;
	public Integer valueInt;
	
	public Grain cloneDefinition()
	{
		Grain g2 = new Grain();
		g2.internalDataType = this.internalDataType;
		g2.columnName = this.columnName;		
		return g2;		
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
