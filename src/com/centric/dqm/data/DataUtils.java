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
	public static final String BOOTSTRAP_VALIDATE_RESOURCE = "bootstrap_validate.sql";
	public static final String SELECT_SCENARIOS_RESOURCE = "select_scenarios.sql";
	public static final String SELECT_SCENARIO_MEASURES_RESOURCE = "select_scenario_measures.sql";
	public static final String INSERT_TEST_RESOURCE = "insert_test.sql";
	public static final String INSERT_TEST_CASE_RESOURCE = "insert_test_case.sql";
	public static final String DELETE_TEST_CASE_RESOURCE = "delete_test_case.sql";
	public static final String SELECT_CURRENT_DATE_RESOURCE = "select_current_date.sql";
	
	public static final int MAX_RESULTSET_ROWS = 10000;
	
	public static Properties getConnectionProperties(String url, String user, String password, int timeout)
	{
		Properties prop = new Properties();
		
		prop.put("url", url);
		prop.put("user", user);
		prop.put("password", password);
		prop.put("timeout", String.valueOf(timeout));
		
		return prop;
	}
	
	public static String delimitSQLString(String value)
	{
		
		if(value == null)
		{
			return "NULL";
		} else
		{
			return "'" + value.replace("'", "''") + "'";
		}
	}
	
	
	public static IConnection getConnectionFromDriver(String driver, String url, String user, String password, int timeout)
	{
		return DataUtils.getConnectionFromDriver(driver, DataUtils.getConnectionProperties(url, user, password, timeout));
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
		return executeCommandWithResult(commandText, CurrentConnection, 0);
	}
	
	public static ResultSet executeCommandWithResult(String commandText, IConnection CurrentConnection, int maxRows)
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
	       
	       if(maxRows > 0)
	       {
	    	   stmt.setMaxRows(maxRows);
	       }
	       
	       
	       // set the query timeout if applicable
	       if(CurrentConnection.getConnectionTimeout() > 0)
	       {
	    	   stmt.setQueryTimeout(CurrentConnection.getConnectionTimeout());
	       }
	       
	       rs = stmt.executeQuery(commandText.trim());
	       	
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
	    
	    if(rs == null)
	    {
	    	return;
	    }
	    
	    // capture the statement object
	    try 
	    {	    	
			stmt =  rs.getStatement();		
	    }
	    catch (SQLException e)
	    {

			e.printStackTrace();
		}	    
	    
	    // capture the connection object
	    try 
	    {
		
	    	con = stmt.getConnection();
	    	
		} catch (SQLException e) {

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
	       
	       List<String> commandTextList = new ArrayList<String>(Arrays.asList(commandText.split(";")));
	       
	       for(String commandTextSegment : commandTextList)
	       {
	       
	    	   stmt.execute(commandTextSegment.trim());
	    	   con.commit();
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
		if(value == null)
		{
			// return empty list
			return new ArrayList<String>();
			
		} else
		{
			
			if(value.trim().length() == 0 || value.trim().equals("\"\""))
			{
				return new ArrayList<String>();
				
			} else
			{			
				return new ArrayList<String>(Arrays.asList(value.trim().split(delimter)));
			}
		}
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
		
		// the resource folder is the relative to the DataUtils package folder path
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



