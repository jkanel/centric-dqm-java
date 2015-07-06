package com.centric.dqm.data;

import java.sql.ResultSet;

public interface IConnection {

	public String getJdbcDriver();
	public String getConnectionUser();
	public String getConnectionPassword();
	public int getConnectionTimeout();
	public String getConnectionUrl();
	
	public ResultSet executeCommandWithResult(String commandText);
	public void executeCommand(String commandText);
	
	
	
		
}
