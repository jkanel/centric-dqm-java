package com.centric.dqm.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.centric.dqm.data.mysql.MySQLConnection;
import com.centric.dqm.data.oracle.OracleConnection;
import com.centric.dqm.data.sqlserver.SqlServerConnection;


public class DataUtils {
	
	public static final String BOOTSTRAP_RESOURCE = "bootstrap.sql";
	public static final String VALIDATE_BOOTSTRAP_RESOURCE = "validate_bootstrap.sql";
	public static final String GET_SCENARIOS_RESOURCE = "get_scenarios.sql";
	public static final String GET_SCENARIO_MEASURES_RESOURCE = "get_scenario_measures.sql";
	public static final String INSERT_TEST_RESOURCE = "insert_test.sql";
	public static final String INSERT_TEST_RESULT_RESOURCE = "insert_test_result.sql";
	
	public static final int MAX_ROWS = 1000;
	
	public static Properties getConnectionProperties(String driver, String user, String password, String database, String server, String port, Boolean trusted)
	{
		Properties prop = new Properties();
		
		prop.put("driver", driver);
		prop.put("user", user);
		prop.put("password", password);
		prop.put("database", database);
		prop.put("server", server);
		prop.put("trusted", Boolean.toString(trusted));
		
		return prop;
	}
	
	public static IConnection getConnectionFromDriver(String driver, Properties properties)
	{
		if(SqlServerConnection.JDBC_DRIVER.equals(driver))
		{
			return new SqlServerConnection(properties);		
		}
		else if (MySQLConnection.JDBC_DRIVER.equals(driver))
		{
			return new MySQLConnection(properties);			
		}
		else if (OracleConnection.JDBC_DRIVER.equals(driver))
		{
			return new OracleConnection(properties);			
		}
		else
		{
			throw new IllegalArgumentException("The specified driver \"" + driver + "\" does not correspond to an available database connection.");
		}
	}

	
	public static ResultSet executeCommandWithResult(String commandText, IConnection CurrentConnection)
	{

	    // Declare the JDBC objects.
	    Connection con = null;
	    Statement stmt = null;
	    ResultSet rs = null;
	
	    try 
	    {
	       // Establish the connection.
	       Class.forName(CurrentConnection.getJdbcDriver());
	       
	       String url = CurrentConnection.getConnectionUrl();
	       String user = CurrentConnection.getConnectionUser();
	       String password = CurrentConnection.getConnectionPassword();
	       
	       con = DriverManager.getConnection(url, user, password);	    		
	
	       // Create and execute an SQL statement that returns some data.
	       stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
	       stmt.setMaxRows(DataUtils.MAX_ROWS);
	       
	       rs = stmt.executeQuery(commandText);
	       	
	    }
	    catch (Exception e)
	    {
	       e.printStackTrace();
	    }
	    
	    return rs;

	}
	
	public static void disposeResulset(ResultSet rs)
	{
		
	    Statement stmt = null;
	    Connection con = null;
	    
	    // capture the statement object
	    try 
	    {	    	
			stmt =  rs.getStatement();		
	    }
	    catch (SQLException e)
	    {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	    
	    
	    // capture the connection object
	    try 
	    {
		
	    	con = stmt.getConnection();
	    	
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    

	    // close all objects
	    finally 
	    {
	       if (rs != null) try { rs.close(); } catch(Exception e) {}
	       if (stmt != null) try { stmt.close(); } catch(Exception e) {}
	       if (con != null) try { con.close(); } catch(Exception e) {}
	    }
	    
		
	}
	
	public static void executeCommand(String commandText, IConnection CurrentConnection)
	{

	    // Declare the JDBC objects.
	    Connection con = null;
	    Statement stmt = null;
	
	    try 
	    {
	       // Establish the connection.
	       Class.forName(CurrentConnection.getJdbcDriver());
	       
	       con = DriverManager.getConnection(
	    		   CurrentConnection.getConnectionUrl(), 
	    		   CurrentConnection.getConnectionUser(), 
	    		   CurrentConnection.getConnectionPassword()
	    		 );
	
	       // Create and execute an SQL statement that returns some data.
	       stmt = con.createStatement();
	       stmt.setMaxRows(DataUtils.MAX_ROWS);
	       
	       List<String> commandTextList = new ArrayList<String>(Arrays.asList(commandText.split(";")));
	       
	       for(String commandTextSegment : commandTextList)
	       {
	       
	    	   stmt.execute(commandTextSegment);
	       }
	       	
	    }
	    catch (Exception e)
	    {
	       e.printStackTrace();
	    }
	    finally 
	    {
	       if (stmt != null) try { stmt.close(); } catch(Exception e) {}
	       if (con != null) try { con.close(); } catch(Exception e) {}
	    }
	       
	}
	
	
	public static List<String> getListFromString(String value)
	{
		return DataUtils.getListFromString(value,",");
	}
	
	public static List<String> getListFromString(String value, String delimter)
	{
		return new ArrayList<String>(Arrays.asList(value.split(delimter)));
	}

	public static boolean listsIntersect(List<String> listA, List<String> listB)
	{
		for(Object a : listA)
		{
			for(Object b : listB)
			{
				if(a.equals(b))
				{
					return true;
				}
			}
		}
		
		return false;
	}
	
	public static String getScriptResource(String resourceFolder, String resourceName) throws FileNotFoundException, IOException
	{
		
		String resourcePath = resourceFolder + "/" + resourceName;
			
		InputStream in = DataUtils.class.getResourceAsStream(resourcePath);

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		// load the file into a result string builder
		
		StringBuilder result = new StringBuilder("");
		
		String line;
		while ((line=reader.readLine()) != null)
		{
		     result.append(line).append("\n");
		}
		
		// return the file contents
		return result.toString();
		
		
	}

}
