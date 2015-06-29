package com.centric.dqm.testing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Measure {
	
	public String columnName;
	public int internalDataType = TestCase.INTERNAL_DATA_TYPE_UNKNOWN;
	
	public int precision;
	
	public Double allowedVariance = 0.0d;
	public Double allowedVarianceRate = 0.0d;
	
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
	
	public boolean isNumeric()
	{
		switch(this.internalDataType)
		{
		
		case TestCase.INTERNAL_DATA_TYPE_NUMERIC:
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			return true;
		
		case TestCase.INTERNAL_DATA_TYPE_DATE:		
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			return false;
			
		default:
			
			return false;
		}
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
			break;
		
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			this.actualValueInt = rs.getInt(this.columnName);
			break;
		
		case TestCase.INTERNAL_DATA_TYPE_DATE:
		
			this.actualValueDate = rs.getDate(this.columnName);
			break;
			
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			this.actualValueText = rs.getString(columnName);
			break;	
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
			break;
		
		case TestCase.INTERNAL_DATA_TYPE_INTEGER:
		
			this.expectedValueInt = rs.getInt(this.columnName);
			break;
			
		case TestCase.INTERNAL_DATA_TYPE_DATE:
		
			this.expectedValueDate = rs.getDate(this.columnName);
			break;
			
		case TestCase.INTERNAL_DATA_TYPE_TEXT:
		
			this.expectedValueText = rs.getString(columnName);
			break;	
		}
		
		_isExpectedValueAssigned = true;
		
	}
			
	public Double getVariance()
	{
		if (this._resultVariance == null)
		{
			switch(this.internalDataType)
			{
			case TestCase.INTERNAL_DATA_TYPE_INTEGER:
				
				
				if(this.actualValueInt == null && this.expectedValueInt == null)
				{
					this._resultVariance = 0.0d;
					break;
				}
				
				
				try
				{
									
					this._resultVariance = (double)(this.actualValueInt - this.expectedValueInt);				
					
				} catch(NullPointerException e)
				{
					this._resultVariance = null;
				}
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_NUMERIC:				
				
				
				if(this.actualValueNumeric == null && this.expectedValueNumeric == null)
				{
					this._resultVariance = 0.0d;
					break;
				}
				
				
				try
				{
					this._resultVariance = (this.actualValueNumeric - this.expectedValueNumeric);
					
				} catch(NullPointerException e)
				{
					this._resultVariance = null;
				}
				
								
				break;
			

			case TestCase.INTERNAL_DATA_TYPE_TEXT:
				
				if(this.actualValueText == null && this.expectedValueText == null)
				{
					this._resultVariance = 0.0d;
					break;
				}
				
				try
				{					
					this._resultVariance = (this.actualValueText.equals(this.expectedValueText)) ? 0.0d : 1.0d;
					
				} catch(NullPointerException e)
				{
					this._resultVariance = null;
				}
				
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_DATE:				
				
				if(this.actualValueDate == null && this.expectedValueDate == null)
				{
					this._resultVariance = 0.0d;
					break;
				}
				
				try
				{					
					this._resultVariance = (this.actualValueDate.equals(this.expectedValueDate)) ? 0.0d : 1.0d;
					
				} catch(NullPointerException e)
				{
					this._resultVariance = null;
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
			
			switch(this.internalDataType)
			{
			case TestCase.INTERNAL_DATA_TYPE_INTEGER:
				
				if(this.actualValueInt == null && this.expectedValueInt == null)
				{
					this._resultVarianceRate = 0.0d;
					break;
				}
				
				try
				{				
				
					Double value = (double)(this.actualValueInt - this.expectedValueInt)/(double)this.expectedValueInt;
					
					// determine the scale to apply to the variance %
					actualScale = new BigDecimal(actualValueInt).scale();
					expectedScale = new BigDecimal(this.expectedValueInt).scale();
					scale = 4 + 2*expectedScale - actualScale;
					
					this._resultVarianceRate =  new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP).doubleValue(); 	
					
				}
				catch(NullPointerException e)
				{
					this._resultVarianceRate = null;
				}
				
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_NUMERIC:				
				
				if(this.actualValueNumeric == null && this.expectedValueNumeric == null)
				{
					this._resultVarianceRate = 0.0d;
					break;
				}
				
				try
				{
					
					Double value = (this.actualValueNumeric - this.expectedValueNumeric)/this.expectedValueNumeric;
					
					// determine the scale to apply to the variance %
					actualScale = new BigDecimal(this.actualValueNumeric).scale();
					expectedScale = new BigDecimal(this.expectedValueNumeric).scale();
					scale = 4 + 2 * expectedScale - actualScale;
									
					this._resultVarianceRate = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP).doubleValue();
					
				}
				catch(NullPointerException e)
				{
					this._resultVarianceRate = null;
				}
				
				break;
			

			case TestCase.INTERNAL_DATA_TYPE_TEXT:			
				
				if(this.actualValueText == null && this.expectedValueText == null)
				{
					this._resultVarianceRate = 0.0d;
					break;
				}
				
				try
				{					
					this._resultVarianceRate = (this.actualValueText.equals(this.expectedValueText)) ? 0.0d : 1.0d;
					
				} catch(NullPointerException e)
				{
					this._resultVarianceRate = null;
				}
				
				break;
				
			case TestCase.INTERNAL_DATA_TYPE_DATE:				
				
				
				if(this.actualValueDate == null && this.expectedValueDate == null)
				{
					this._resultVarianceRate = 0.0d;
					break;
				}
				
				
				try
				{					
					this._resultVarianceRate = (this.actualValueDate.equals(this.expectedValueDate)) ? 0.0d : 1.0d;
					
				} catch(NullPointerException e)
				{
					this._resultVarianceRate = null;
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
	
	public int failureCount()
	{
		return ((this.isFailed() == true) ? 1 : 0);
	}
	
	public int successCount()
	{
		return ((this.isFailed() == false) ? 1 : 0);
	}
	
	public boolean isFailed()
	{
		if(this._isOutOfRange == null)
		{
			this._isOutOfRange = (this.isVarianceOutOfRange() || this.isVarianceRateOutOfRange());	
		}
		
		return _isOutOfRange;
	}
	

	
	public boolean isVarianceOutOfRange()
	{
		boolean result;
		Double variance = this.getVariance();
		
		if(variance == null)
		{
			result = true;
			
		} else if(this.allowedVariance == null)
		{
			// assume allowed variance of zero
			result = (boolean)(Math.abs(variance) > 0.0d);		
		}
		else
		{			
						
			result = (boolean)(Math.abs(variance) > Math.abs(this.allowedVariance));			
		}
		
		return result;
	}
	
	public boolean isVarianceRateOutOfRange()
	{
		boolean result;
		Double varianceRate = this.getVarianceRate();
		
		if(varianceRate == null)
		{
			result = true;
			
		} else if(this.allowedVarianceRate == null)
		{
			// assume allowed varianceRate of zero
			result = (boolean)(Math.abs(varianceRate) > 0.0d);		
		}
		else
		{			
						
			result = (boolean)(Math.abs(varianceRate) > Math.abs(this.allowedVarianceRate));			
		}
		
		return result;
		
	}
	
	public String actualValueToString()
	{
		try
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
		} catch(NullPointerException e)
		{
			return (String)null;
		}
	}
	
	public String expectedValueToString()
	{
		
		try
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
			
		} catch(NullPointerException e)
		{
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
