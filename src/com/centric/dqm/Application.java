package com.centric.dqm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;

import com.centric.dqm.data.DataUtils;
import com.centric.dqm.testing.Harness;

public class Application {
	
	public static void main(String[] args) throws IOException, SQLException
	{
		
    	/*
    	 * -c "{Configuration File Path (String)}"
    	 * -t "{Tag (String)}"
    	 * -s "{Scenario Identifier (String)}" 
    	 */
		
		
    	
    	String tags = null;
    	String scenarioIdentifiers = null;
    	
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
    	    	
    	Configuration config = new Configuration();
    	Harness harness = new Harness();
    	
    	harness.IdentifierList = DataUtils.getListFromString(scenarioIdentifiers);
    	harness.TagList = DataUtils.getListFromString(tags);
				    	
    	config.Connection.readHarness(harness);
    	
    	harness.perfomTests();
    	
    	config.Connection.writeHarness(harness);
    	
    	
	}
	
	public static String getJarPath() throws UnsupportedEncodingException
	{
		String path = Application.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		String decodedPath = URLDecoder.decode(path, "UTF-8");
		
		return decodedPath;
	}
	    
}
