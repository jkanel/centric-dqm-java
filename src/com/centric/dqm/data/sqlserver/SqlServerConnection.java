package com.centric.dqm.data.sqlserver;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.IConnection;
import com.centric.dqm.testing.Harness;

public class SqlServerConnection implements IConnection  {

	public final static String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public final static String SCRIPT_RESOURCE_FOLDER = "sqlserver";
	
	public String user = null;
	public String password = null;
	public String jdbcUrl = null;
	
	public SqlServerConnection(){}

	public SqlServerConnection(String jdbcUrl)
	{
		this.jdbcUrl = jdbcUrl;
	}
		
	public SqlServerConnection(Properties properties)
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
		
		/*
		 
		 		String msg = "Hello {0} Please find attached {1} which is due on {2}";
String[] values = {
  "John Doe", "invoice #123", "2009-06-30"
};
System.out.println(MessageFormat.format(msg, values));
		 		
		 */
	}
	
	public void bootstrap() throws FileNotFoundException, IOException, SQLException
	{
		String commandText;
		ResultSet rs;
		
		// execute the test for existing tables
		commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.VALIDATE_BOOTSTRAP_RESOURCE);		
		rs = DataUtils.executeCommandWithResult(commandText, this);
		
		int rowCount = 0;
		
		boolean result = rs.next();
		if(result == true)
		{
			rowCount = rs.getInt(1);
		}
		
		// dispose of resultset
		DataUtils.disposeResulset(rs);

		
		// if not records are returned, the conduct bootstrap
		if(rowCount == 0)
		{
			commandText = DataUtils.getScriptResource(SqlServerConnection.SCRIPT_RESOURCE_FOLDER, DataUtils.BOOTSTRAP_RESOURCE);		
			DataUtils.executeCommand(commandText, this);			
		}
		
	}

}
