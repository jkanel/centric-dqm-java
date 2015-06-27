package com.centric.dqm.data.oracle;


import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.IConnection;
import com.centric.dqm.data.sqlserver.SqlServerConnection;
import com.centric.dqm.testing.Harness;


public class OracleConnection implements IConnection  {

	public final static String JDBC_DRIVER = "oracle.jdbc.OracleDriver";
	public final static String SCRIPT_RESOURCE_FOLDER = "oracle";
	
	public String user = null;
	public String password = null;
	public String jdbcUrl = null;
	
	public OracleConnection(){}
	
	public OracleConnection(String jdbcUrl)
	{
		this.jdbcUrl = jdbcUrl;
	}
		
	public OracleConnection(Properties properties)
	{
		applyProperties(properties);
	}

	
	public void applyProperties(Properties properties)
	{
		this.jdbcUrl = properties.getProperty("url");
		this.user = properties.getProperty("user");
		this.password = properties.getProperty("password");
		
		// check for missing properties and throw error if needed
		List<String> errorList = new ArrayList<String>();
		
		if(this.user.length() == 0)
		{
			errorList.add("user");
		}
		
		if(this.password.length() == 0)
		{
			errorList.add("password");
		}
		
		if(this.jdbcUrl.length() == 0)
		{
			errorList.add("url");
		}
		
		if(errorList.size() > 0)
		{
			throw new IllegalArgumentException("The following properties must be specified: " + StringUtils.join(errorList, ","));
		}
				
	}
	
	public String getConnectionUrl()
	{		
		return this.jdbcUrl;
	}


	public String getJdbcDriver(){
		return SqlServerConnection.JDBC_DRIVER;
	}
	
	public String getConnectionUser()
	{
		return this.user;
	}
	
	public String getConnectionPassword()
	{
		return this.password;		
	}	
	
	public ResultSet executeCommandWithResult(String commandText)
	{
	    return DataUtils.executeCommandWithResult(commandText, this);	    
	}
	
	public void executeCommand(String commandText)
	{
	    DataUtils.executeCommand(commandText, this);	    
	}
	
	
	public void readHarness(Harness harness)
	{
		
	}
	
	public void writeHarness(Harness harness)
	{
		
	}
	
	public void bootstrap()
	{
		
	}


}