package com.centric.dqm;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.CodeSource;

import com.centric.dqm.data.Bootstrapper;
import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.HarnessReader;
import com.centric.dqm.data.HarnessWriter;
import com.centric.dqm.testing.Harness;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Application {
	
	public static final Logger logger = LogManager.getLogger();
	
	public static void main(String[] args) throws Exception
	{
		
    	/*
    	 * -c "{Configuration File Path (String)}"
    	 * -t "{Tag (String)}"
    	 * -s "{Scenario Identifier (String)}" 
    	 */

		// #################################################
        logger.info("Entering application");
		
        // #################################################
        logger.info("Interpreting command line parameters"); 
    	
    	String tags = null;
    	String scenarioIdentifiers = null;
        
    	try
    	{
	    	
	    	for (int n = 0; n < args.length; n++)
	    	{
	    		
	    		if(args[n].equals("-t"))
	    		{
	    			tags = args[n+1];
	    			n++;  // advance the arg counter    			
	    			
	    		} else if(args[n].equals("-s"))
	    		{
	    			scenarioIdentifiers = args[n+1];
	    			n++;  // advance the arg counter    		    	
	    		}    		    		
	    	}
	    	
    	} catch (Exception e)
    	{
    		logger.error("Encountered exception.", e);
    		throw e;
    	} 
    	
    	logger.info("Tags: " + ((tags == null || tags.length()==0) ? "(not specified)" : tags));
    	logger.info("Scenarios: " + ((scenarioIdentifiers == null || scenarioIdentifiers.length()==0) ? "(not specified)" : scenarioIdentifiers));
    	
    	// #################################################
    	logger.info("Checking that management database exists at");
    	
    	Configuration config = null;
    	
    	try
    	{
    		config = new Configuration();
    		
    		if(Bootstrapper.isBootstrapped(config.Connection)==false)
    		{
    	
    			logger.info("Establishing management database");
    			logger.info("Driver: " + config.Connection.getJdbcDriver()); 
    			logger.info("Url: " + config.Connection.getConnectionUrl()); 
    			
    			Bootstrapper.bootstrap(config.Connection);
    			
    			logger.info("Management database has been created");
    			logger.info("Exiting the application");    			
    			return;
    		}
    		
    		
    		
    	} catch(Exception e)
    	{
    		logger.error("Encountered exception.", e);

    		throw e;
    	}
    	
    	// #################################################
    	logger.info("Initiating testing harness");    	
    	Harness harness = null;
    	
    	try
    	{

        	harness = new Harness();
        	
        	harness.ScenarioFilterList = DataUtils.getListFromString(scenarioIdentifiers);
        	harness.TagList = DataUtils.getListFromString(tags);
        	
        	HarnessReader.readHarness(config.Connection, harness);
    		
    	} catch(Exception e)
    	{
    		logger.error("Encountered exception.", e);
    		throw e;
    	}
    	

    	// #################################################
    	logger.info("Performing tests");    	
    	try
    	{

    		harness.perfomTests();
    		
    	} catch(Exception e)
    	{
    		logger.error("Encountered exception.", e);
    		throw e;
    	}
    	
    	// #################################################
    	try
    	{

    		HarnessWriter.writeHarness(config.Connection, harness);
    		
    	} catch(Exception e)
    	{
    		logger.error("Encountered exception.", e);
    		throw e;
    	}    	

    	
    	// #################################################
    	logger.info("Exiting the application");
    	
    	
	}
	
	public static String getJarPath() throws UnsupportedEncodingException, URISyntaxException
	{
		//String path = Application.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		//String path = ClassLoader.getSystemClassLoader().getResource(".").getPath();
		//String decodedPath = URLDecoder.decode(path, "UTF-8");
		//return decodedPath;
		
		CodeSource codeSource = Application.class.getProtectionDomain().getCodeSource();
		File jarFile = new File(codeSource.getLocation().toURI().getPath());
		String jarDir = jarFile.getParentFile().getPath();
		
		return jarDir;
	}
	
	public static String getExceptionStackTrace(Exception ex)
	{
		return Application.getExceptionStackTrace(ex, 0);
	}
	
	public static String getExceptionStackTrace(Exception ex, int maxLength)
	{
		StringWriter trace = new StringWriter();
		ex.printStackTrace(new PrintWriter(trace));
		
		if(maxLength > trace.toString().length())
		{
			maxLength = trace.toString().length();
		}
		
		if(maxLength == 0)
		{
			return trace.toString();	
		} else
		{
			return trace.toString().substring(0, maxLength - 1);
		}
		
	}
	    
}
