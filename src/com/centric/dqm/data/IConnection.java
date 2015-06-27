package com.centric.dqm.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.centric.dqm.testing.Harness;

public interface IConnection {

	public String getJdbcDriver();
	public String getConnectionUser();
	public String getConnectionPassword();
	
	public void bootstrap() throws FileNotFoundException, IOException, SQLException;
	public ResultSet executeCommandWithResult(String commandText);
	public void executeCommand(String commandText);
	public void applyProperties(Properties properties);
	public void readHarness(Harness harness);
	public void writeHarness(Harness harness);
	public String getConnectionUrl();
	
}
