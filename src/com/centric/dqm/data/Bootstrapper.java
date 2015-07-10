package com.centric.dqm.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Bootstrapper {
	
	public static void assertBootstrap(IConnection con) throws Exception
	{
		if(Bootstrapper.isBootstrapped(con) == false)
		{
			Bootstrapper.bootstrap(con);		
		}
	}
	
	public static void bootstrap(IConnection con) throws FileNotFoundException, IOException, SQLException
	{
		String commandText;

		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.BOOTSTRAP_RESOURCE);		
		DataUtils.executeCommand(commandText, con);			
	
	}
	
	public static boolean isBootstrapped(IConnection con) throws Exception
	{
		String commandText;
		ResultSet rs;
		
		// execute the test for existing tables
		commandText = DataUtils.getScriptResource(con.getJdbcDriver(), DataUtils.BOOTSTRAP_VALIDATE_RESOURCE);		
		rs = DataUtils.executeCommandWithResult(commandText, con);
		
		int rowCount = 0;
		
		boolean result = rs.next();
		if(result == true)
		{
			rowCount = rs.getInt(1);
		}
		
		// dispose of resultset
		DataUtils.disposeResulset(rs);
		
		return (boolean)(rowCount > 0);
	}
	

	
	

}
