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

import com.centric.dqm.data.generic.GenericConnection;

public class DataUtils {
	
	public static final String BOOTSTRAP_RESOURCE = "bootstrap.sql";
	public static final String BOOTSTRAP_VALIDATE_RESOURCE = "bootstrap_validate.sql";
	public static final String SELECT_SCENARIOS_RESOURCE = "select_scenarios.sql";
	public static final String SELECT_SCENARIO_MEASURES_RESOURCE = "select_scenario_measures.sql";
	public static final String INSERT_TEST_RESOURCE = "insert_test.sql";
	public static final String INSERT_TEST_CASE_RESOURCE = "insert_test_case.sql";
	public static final String DELETE_TEST_CASE_RESOURCE = "delete_test_case.sql";
	public static final String SELECT_CURRENT_DATE_RESOURCE = "select_current_date.sql";
	public static final String UPDATE_SCENARIO_QUERY = "update_scenario_query.sql";
	
	public final static String SQL_SERVER_RESOURCE_FOLDER = "sqlserver";
	public final static String MYSQL_RESOURCE_FOLDER = "mysql";
	public final static String ORACLE_RESOURCE_FOLDER = "oracle";
		
	public final static String SQL_SERVER_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public final static String MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";
	public final static String ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver";
	
	public final static String JDBC_DRIVER_PROPERTY = "driver";
	public final static String JDBC_URL_PROPERTY = "url";
	public final static String USER_PROPERTY = "user";
	public final static String PASSWORD_PROPERTY = "password";
	public final static String TIMEOUT_PROPERTY = "timeout";
	
	
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
	
	
	public static IConnection getConnection(String driver, String url, String user, String password, int timeout)
	{
		return new GenericConnection(driver, url, user, password, timeout);
	}
	
	public static IConnection getConnection(Properties prop)
	{
		
		int timeout = 0;
		
		try
		{
			timeout = Integer.parseInt(prop.getProperty(DataUtils.TIMEOUT_PROPERTY));
		} catch (Exception e)
		{
			// no action
			timeout = 0;			
		}
		
		return new GenericConnection(
			prop.getProperty(DataUtils.JDBC_DRIVER_PROPERTY).trim(),
			prop.getProperty(DataUtils.JDBC_URL_PROPERTY).trim(),
			prop.getProperty(DataUtils.USER_PROPERTY).trim(),
			prop.getProperty(DataUtils.PASSWORD_PROPERTY).trim(),
			timeout
			);
	}
	

	public static String getSourceFolderFromDriver(String driver)
	{
		if(DataUtils.SQL_SERVER_JDBC_DRIVER.equals(driver))
		{
			return DataUtils.SQL_SERVER_RESOURCE_FOLDER;	
		}
		else if (DataUtils.MYSQL_JDBC_DRIVER.equals(driver))
		{
			return DataUtils.MYSQL_RESOURCE_FOLDER;		
		}
		else if (DataUtils.ORACLE_JDBC_DRIVER.equals(driver))
		{
			return DataUtils.ORACLE_RESOURCE_FOLDER;		
		}
		else
		{
			throw new IllegalArgumentException("The specified driver \"" + driver + "\" does not resolve to a resource folder.");
		}
	}
	
	public static ResultSet executeCommandWithResult(String commandText, IConnection CurrentConnection) throws Exception
	{
		// no max rows are specified
		return executeCommandWithResult(commandText, CurrentConnection, 0);
	}
	
	public static ResultSet executeCommandWithResult(String commandText, IConnection CurrentConnection, int maxRows) throws Exception
	{

	    // Declare the JDBC objects.
	    Connection con = null;
	    Statement stmt = null;
	    ResultSet rs = null;

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
		executeCommand(commandText, CurrentConnection, ";");
	}
	
	public static void executeCommand(String commandText, IConnection CurrentConnection, String parseChar)
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
	       
	       // if no parsing character is provided, directly execute the command
	       if(parseChar == null)
	       {
	    	   stmt.execute(commandText.trim());
	    	   con.commit();
	    	   
	       // otherwise parse into multiple commands
	       } else
	       {	    	
	    	   
	    	   List<String> commandTextList = new ArrayList<String>(Arrays.asList(commandText.split(parseChar)));
	    	   
	    	   for(String commandTextSegment : commandTextList)
		       {
		       
		    	   stmt.execute(commandTextSegment.trim());
		    	   con.commit();
		       }	    	   
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
	
	
	public static List<String> getListFromLowerCaseString(String value)
	{
		if (value==null)
		{
			return new ArrayList<String>();
			
		} else
		{
			return DataUtils.getListFromString(value.toLowerCase(),",", null);
		}
		
	}
	
	public static List<String> getListFromString(String value)
	{
		return DataUtils.getListFromString(value,",", null);
	}
	
	public static List<String> getListFromString(String value, String delimiter)
	{
	  return DataUtils.getListFromString(value, delimiter, null);
	}
	
	public static List<String> getListFromString(String value, String delimiter, String removeChars)
	{
		
		// set regex to split on delimiter and whitepsace
		String delimiterRegex = "\\s*" + delimiter + "+\\s*"; 
		
		if (removeChars != null && removeChars.length() > 0)
		{
			for(String rc : removeChars.split(""))
			{
				value = value.replace(rc,"");
			}
		}
		
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
				return new ArrayList<String>(Arrays.asList(value.trim().split(delimiterRegex)));
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
	
	public static String getScriptResource(String driver, String resourceName) throws FileNotFoundException, IOException
	{
		
		String resourceFolder = DataUtils.getSourceFolderFromDriver(driver);
		
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



