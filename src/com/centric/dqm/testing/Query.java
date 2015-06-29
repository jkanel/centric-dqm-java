package com.centric.dqm.testing;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.IConnection;

public class Query {	

	
	public static final String COMMAND_PARAM_CURRENT_TIME = "<<CURRENT_TIME>>";
	public static final String COMMAND_PARAM_CURRENT_DATE = "<<CURRENT_DATE>>";
	public static final String COMMAND_PARAM_CURRENT_YEAR = "<<CURRENT_YEAR>>";
	public static final String COMMAND_PARAM_MODULUS = "<<MODULUS>>";
	public static final String COMMAND_PARAM_MODULARITY = "<<MODULARITY>>";
	
	public String errorMessage;
	public Integer errorNumber;
	
	public String commandText;	
	
	public Exception queryException = null;
	
	public Query(){}
		
	public ResultSet execute(IConnection connection)
	{		
		return this.execute(connection, 1, 0);
	}
	
	public ResultSet execute (IConnection connection, Integer modulus, Integer modularity)
	{
		
		try
		{
			String parameterizeCommandText = Query.parameterizeCommandText(this.commandText, modulus, modularity);		
			return connection.executeCommandWithResult(parameterizeCommandText);
				
		} catch(Exception e)
		{
			this.queryException = e;
			return null;
		}
		
	}
	
	public static String parameterizeCommandText(String commandText, Integer modulus, Integer modularity)
	{
		
		Date currentDate = new Date();
		
		SimpleDateFormat SdfDatetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat SdfDate = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat SdfYear = new SimpleDateFormat("yyyy");

		String value = commandText;
		
		value = value.replace(COMMAND_PARAM_CURRENT_TIME, DataUtils.delimitSQLString(SdfDatetime.format(currentDate)));		
		value = value.replace(COMMAND_PARAM_CURRENT_TIME,  DataUtils.delimitSQLString(SdfDate.format(currentDate)));
		value = value.replace(COMMAND_PARAM_CURRENT_YEAR,  SdfYear.format(currentDate));
		
		if(modulus == null || modularity == null || modulus <= 0 || modularity < 0)
		{
			value = value.replace(COMMAND_PARAM_MODULUS, "1");
			value = value.replace(COMMAND_PARAM_MODULARITY,  "0");
			
		} else
		{
			value = value.replace(COMMAND_PARAM_MODULUS, String.valueOf(modulus));
			value = value.replace(COMMAND_PARAM_MODULARITY,  String.valueOf(modularity));
		}
		
		return value;
		
	}
	
}
