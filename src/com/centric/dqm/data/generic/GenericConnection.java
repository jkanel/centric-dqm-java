package com.centric.dqm.data.generic;

import java.sql.ResultSet;

import com.centric.dqm.Configuration;
import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.IConnection;

public class GenericConnection implements IConnection {
	
	public String user = null;
	public String password = null;
	public String jdbcUrl = null;
	public String jdbcDriver = null;
	public int timeout = -1;

	public GenericConnection() {}
	
	public GenericConnection(String driver, String url, String user, String password, int timeout)
	{
		this.jdbcDriver = driver;
		this.jdbcUrl = url;
		this.user= user;
		this.password = password;
		this.timeout = timeout;		
	}
	
	
	@Override
	public String getJdbcDriver() {
		// TODO Auto-generated method stub
		return this.jdbcDriver;
	}

	@Override
	public String getConnectionUser() {
		// TODO Auto-generated method stub
		return this.user;
	}

	@Override
	public String getConnectionPassword() {
		// TODO Auto-generated method stub
		return this.password;
	}

	@Override
	public int getConnectionTimeout() {
		// TODO Auto-generated method stub
		return this.timeout;
	}

	@Override
	public ResultSet executeCommandWithResult(String commandText) throws Exception {
		// TODO Auto-generated method stub
		return DataUtils.executeCommandWithResult(commandText, this, Configuration.maxResultsetRows);
	}

	@Override
	public void executeCommand(String commandText) throws Exception {
		// TODO Auto-generated method stub
		DataUtils.executeCommand(commandText, this);
		
	}

	@Override
	public String getConnectionUrl() {
		// TODO Auto-generated method stub
		return this.jdbcUrl;
	}

}
