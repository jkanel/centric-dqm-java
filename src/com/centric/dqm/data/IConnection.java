package com.centric.dqm.data;

import java.sql.ResultSet;
import java.util.Properties;


public interface IConnection {

	public String getJdbcDriver();
	public String getConnectionUser();
	public String getConnectionPassword();
	public int getConnectionTimeout();
	
	public ResultSet executeCommandWithResult(String commandText);
	public void executeCommand(String commandText);
	public void applyProperties(Properties properties);
	public String getConnectionUrl();
	
	public String getScriptResourceFolder();

	
}
