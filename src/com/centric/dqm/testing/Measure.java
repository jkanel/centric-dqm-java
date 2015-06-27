package com.centric.dqm.testing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Measure {
	
	public String columnName;
	public int internalDataType;
	
	public int precision;
	
	public Double allowedVariance;
	public Double allowedVarianceRate;
	
	protected Double _resultVariance = null;
	protected Double _resultVarianceRate = null;
	protected Boolean _isOutOfRange = null;
	protected boolean _isActualValueAssigned = false;
	protected boolean _isExpectedValueAssigned = false;
	
	public Double actualValueNumeric;
	public String actualValueText;
	public Date actualValueDate;
	public Integer actualValueInt;
		
	public Double expectedValueNumeric;
	public String expectedValueText;
	public Date expectedValueDate;
	public Integer expectedValueInt;

	public Measure() {}
	
	public Measure(String columnName) {
		this.columnName = columnName;
	}
	
	public void assignActualValue(ResultSet rs) throws SQLException
	{
		if(this._isActualValueAssigned == true)
		{
			throw new IllegalStateException("The actual value for " + this.columnName 
					+ " was already assigned.  This may indicate multiple records having the same grain.");
		}
		
		switch(this.internalDataType)
		{
		
		case TestCase.INTERNAL_DATA_TYPE_NUMERIC:
		
			this.actualValueNumeric = rs.getDouble(this.columnName);
		
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			this.actualValueInt = rs.getInt(this.columnName);
		
		case TestCase.INTERNAL_DATA_TYPE_DATE:
		
			this.actualValueDate = rs.getDate(this.columnName);
		
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			this.actualValueText = rs.getString(columnName);
				
		}
		
		_isActualValueAssigned = true;
		
	}
	
	public void assignExpectedValue(ResultSet rs) throws SQLException
	{
		
		if(this._isExpectedValueAssigned == true)
		{
			throw new IllegalStateException("The expected value for " + this.columnName 
					+ " was already assigned.  This may indicate multiple records having the same grain.");
		}
		
		switch(this.internalDataType)
		{
		
		case TestCase.INTERNAL_DATA_TYPE_NUMERIC:
		
			this.expectedValueNumeric = rs.getDouble(this.columnName);
		
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			this.expectedValueInt = rs.getInt(this.columnName);
		
		case TestCase.INTERNAL_DATA_TYPE_DATE:
		
			this.expectedValueDate = rs.getDate(this.columnName);
		
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			this.expectedValueText = rs.getString(columnName);
				
		}
		
		_isExpectedValueAssigned = true;
		
	}
	
	public void resetResultVariances()
	{
		this._resultVarianceRate = null;
		this._resultVariance = null;		
	}
		
	public Double getVariance()
	{
		if (this._resultVariance == null)
		{
			switch(this.internalDataType)
			{
			case TestCase.INTERNAL_DATA_TYPE_INTEGER:
				
				this._resultVariance = (double)(this.actualValueInt - this.expectedValueInt);						
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_NUMERIC:				
				
				this._resultVariance = (this.actualValueNumeric - this.expectedValueNumeric);				
				break;
			

			case TestCase.INTERNAL_DATA_TYPE_TEXT:				
				
				if(this.actualValueText.equals(this.expectedValueText))
				{
					this._resultVariance = 0.0d;					
				}
				else
				{
					this._resultVariance = 1.0d;
				}
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_DATE:				
				
				if(this.actualValueDate.equals(this.expectedValueDate))
				{
					this._resultVariance = 0.0d;					
				}
				else
				{
					this._resultVariance = 1.0d;
				}
				break;				
				
			default:
				
				this._resultVariance = Double.NaN;
				break;			
			}			
		}
		
		return _resultVariance;
	}
	
	public Double getVarianceRate()
	{
		
		
		if (this._resultVarianceRate == null)
		{
		
			int actualScale;
			int expectedScale;
			int scale;
			Double value;
			
			switch(this.internalDataType)
			{
			case TestCase.INTERNAL_DATA_TYPE_INTEGER:
				
				Integer adjustedActualValueInt = this.actualValueInt ;
				
				value = (double)(adjustedActualValueInt - this.expectedValueInt)/(double)this.expectedValueInt;
				
				// determine the scale to apply to the variance %
				actualScale = new BigDecimal(adjustedActualValueInt).scale();
				expectedScale = new BigDecimal(this.expectedValueInt).scale();
				scale = 4 + 2*expectedScale - actualScale;
				
				this._resultVarianceRate =  new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP).doubleValue(); 						
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_NUMERIC:				
				
				value = (this.actualValueNumeric - this.expectedValueNumeric)/this.expectedValueNumeric;
				
				// determine the scale to apply to the variance %
				actualScale = new BigDecimal(this.actualValueNumeric).scale();
				expectedScale = new BigDecimal(this.expectedValueNumeric).scale();
				scale = 4 + 2 * expectedScale - actualScale;
								
				this._resultVarianceRate = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP).doubleValue(); 				
				break;
			

			case TestCase.INTERNAL_DATA_TYPE_TEXT:				
				
				if(this.actualValueText.equals(this.expectedValueText))
				{
					this._resultVarianceRate = 0.0d;					
				}
				else
				{
					this._resultVarianceRate = 1.0d;
				}
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_DATE:				
				
				if(this.actualValueDate.equals(this.expectedValueDate))
				{
					this._resultVarianceRate = 0.0d;					
				}
				else
				{
					this._resultVarianceRate = 1.0d;
				}
				break;				
				
			default:
				
				this._resultVarianceRate = Double.NaN;
				break;			
			}			
		}
		
		// determine the scale of the actual/expected
		
		return _resultVarianceRate;
		
	}
	
	public boolean isOutOfRange()
	{
		if(this._isOutOfRange == null)
		{
			this._isOutOfRange = (this.isVarianceOutOfRange() || this.isVarianceRateOutOfRange());	
		}
		
		return _isOutOfRange;
	}
	
	public int failureCount()
	{
		return ((this.isOutOfRange() == true) ? 1 : 0);
	}
	
	public int successCount()
	{
		return ((this.isOutOfRange() == false) ? 1 : 0);
	}
	
	public boolean isVarianceOutOfRange()
	{
		if(this.allowedVariance == null)
		{
			// assume allowed variance of zero
			return (this.getVariance().compareTo((double)0.0) == 0);		
		}
		else
		{			
						
			return (this.getVariance().compareTo(this.allowedVariance) == 0);			
		}
	}
	
	public boolean isVarianceRateOutOfRange()
	{
		if(this.allowedVarianceRate == null)
		{
			// assume allowed variance rate of zero
			return (this.getVarianceRate().compareTo((double)0.0) == 0);			
					
		}
		else
		{
			return (this.getVarianceRate().compareTo(this.allowedVarianceRate) == 0);			
				
		}
		
	}
	
	public String actualValueToString()
	{
		
		switch(this.internalDataType)
		{
		
		case TestCase.INTERNAL_DATA_TYPE_NUMERIC:
		
			return this.actualValueNumeric.toString();
		
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			return this.actualValueInt.toString();
		
		case TestCase.INTERNAL_DATA_TYPE_DATE:
		
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
			return dateFormatter.format(this.actualValueDate);
		
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			return this.actualValueText;
		default:
			
			return (String)null;
			
		}
		
	}
	
	public String expectedValueToString()
	{
		
		switch(this.internalDataType)
		{
		
		case TestCase.INTERNAL_DATA_TYPE_NUMERIC:
		
			return this.expectedValueNumeric.toString();
		
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			return this.expectedValueInt.toString();
		
		case TestCase.INTERNAL_DATA_TYPE_DATE:
		
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
			return dateFormatter.format(this.expectedValueDate);
		
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			return this.expectedValueText;
		default:
			
			return (String)null;
			
		}
		
	}
		
	
	public Measure cloneDefinition() 
	{
		Measure m2 = new Measure();
		
		m2.columnName = this.columnName;
		m2.precision = this.precision;
		m2.allowedVariance = this.allowedVariance;
		m2.allowedVarianceRate = this.allowedVarianceRate;
		m2.internalDataType = this.internalDataType;
		
		return m2;
	}
	
}
