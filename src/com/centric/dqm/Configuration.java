package com.centric.dqm;

import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.IConnection;







import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

public class Configuration {
	
	public final static String CONFIG_FILENAME = "com.centric.dqm.properties";
	public final static String MAXROWS_PROPERTY = "maxrows";
	
	public static IConnection Connection;
	
	public static int maxResultsetRows = 0;
	
	public Configuration() throws IOException, SQLException, URISyntaxException
	{
		readConfiguration();
	}
	
	/**
	 * Reads the configuration file (Configuration.CONFIG_FILENAME)
	 * and establishes a connection to the Data Quality Monitoring database.
	 * @throws SQLException 
	 * @throws URISyntaxException 
	*/
	public static void readConfiguration() throws IOException, SQLException, URISyntaxException {

		Properties prop = new Properties();
		String path = Application.getJarPath();
		
		File configFile = new File(path, Configuration.CONFIG_FILENAME);
		
		InputStream inputStream = FileUtils.openInputStream(configFile);
		 
		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException("The configuration file \"" + Configuration.CONFIG_FILENAME + "\" not found in the classpath.");
		}
 
		Configuration.Connection = DataUtils.getConnection(prop);	
		
		// get max rows property
		try
		{
			Configuration.maxResultsetRows = Integer.parseInt(prop.getProperty(Configuration.MAXROWS_PROPERTY));
		}
		catch (Exception e)
		{
			Configuration.maxResultsetRows = 0;
		}
		finally
		{
			if(Configuration.maxResultsetRows < 0)
			{
				Configuration.maxResultsetRows = 0;
			}
		}
		
		
		
		
		
	}	
		
	
}
