package com.centric.dqm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import com.centric.dqm.testing.Scenario;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class Application {
	
	static final String fileSeparator = System.getProperty("file.separator");
	
	public static final Logger logger = LogManager.getLogger();
	
	public static void main(String[] args) throws Exception
	{
		
    	/*
    	 * -c "{Configuration File Path (String)}"
    	 * -t "{Tag (String, Comma Delimited)}"
    	 * -s "{Scenario Identifier (String, Comma Delimited)}"
    	 * -p {Age (days) after which test cases are purged} 
    	 */
		
		// #################################################
        logger.info("Entering application");
		
        // #################################################
        logger.info("Interpreting command line parameters"); 
    	
    	String tags = null;
    	String scenarioIdentifiers = null;
    	Integer purgeDays = null;
        
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
	    			
	    		} else if(args[n].equals("-p"))
	    		{
	    			purgeDays = Integer.parseInt(args[n+1]);
	    			
	    			// set to null of purge days = 0
	    			if(purgeDays < 0)
	    			{
	    				purgeDays = null;
	    			}
	    			
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
    	logger.info("Checking that management database exists");
    	
    	try
    	{
    		Configuration.readConfiguration();
    		
    		if(Bootstrapper.isBootstrapped(Configuration.Connection)==false)
    		{
    	
    			logger.info("Establishing management database");
    			logger.info("Driver: " + Configuration.Connection.getJdbcDriver()); 
    			logger.info("Url: " + Configuration.Connection.getConnectionUrl()); 
    			
    			Bootstrapper.bootstrap(Configuration.Connection);
    			
    			logger.info("Management database has been created");
    			logger.info("Exiting the application"); 
    			
    			// exit the application
    			return;
    			
    		} else if (purgeDays != null)
    		{
    			HarnessWriter.deleteTestCase(Configuration.Connection, purgeDays);
    		}
    		
    		
    		
    	} catch(Exception e)
    	{
    		logger.error("Encountered exception.", e);

    		throw e;
    	}
    	
    	// #################################################
    	logger.info("Importing scenarios..."); 
    	
    	// #################################################
    	logger.info("Initiating testing harness");    	
    	Harness harness = null;
    	
    	try
    	{

        	harness = new Harness();
        	
        	harness.ScenarioFilterList = DataUtils.getListFromString(scenarioIdentifiers);
        	harness.TagList = DataUtils.getListFromString(tags);
        	
        	HarnessReader.readHarness(Configuration.Connection, harness);
    		
    	} catch(Exception e)
    	{
    		logger.error("Encountered exception.", e);
    		throw e;
    	}
    	

    	// #################################################
    	logger.info("Performing tests");    	
    	
    	try
    	{

    		for(Scenario sc : harness.getMatchScenarios())
    		{
    			
    			// perform and write the tests
    			sc.performTest();
    			HarnessWriter.writeTest(Configuration.Connection, sc);
    			
    			// dispose of the scenarios
    			sc.dispose();
    			
    		}
    		
    		
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
	
	public static boolean fileExists(String filePath)
	{
		try
		{
			File f = new File(filePath);
			return (f.isFile());
			
		} catch (Exception e)
		{
			// do nothing
		}
		
		return false;
		
	}
	
	public static boolean directoryExists(String path)
	{
		try
		{
			File f = new File(path);
			return (f.isDirectory());
			
		} catch (Exception e)
		{
			// do nothing
		}
		
		return false;
		
	}
	
	
	public static String getJarFullyQualifiedPath(String relativePath) throws UnsupportedEncodingException, URISyntaxException
	{
		String fileSeparator =  System.getProperty("file.separator");
		
		if(relativePath.startsWith(fileSeparator))
		{
			return Application.getJarPath() + relativePath;
			
		} else
		{
			return Application.getJarPath() + fileSeparator + relativePath;
		}
	
	}

	
	public static String getRelativePath(String relativePath) throws UnsupportedEncodingException, URISyntaxException
	{
		String fileSeparator =  System.getProperty("file.separator");
		
		if(relativePath.startsWith(fileSeparator))
		{
			return Application.getJarPath() + relativePath;
			
		} else
		{
			return Application.getJarPath() + fileSeparator + relativePath;
		}
	
	}
	
	public static String getDirectoryPath(String filePath)
	{

		try
		{
			File f = new File(filePath);
			if(f.isDirectory())
			{

				return filePath;
				
			} else	
			{
				String p = f.getParentFile().getPath();
				return p;
			}
			
		} catch (Exception e)
		{
			// do nothing
		}
		
		return null;
	}
	
	public static String getFileName(String filePath)
	{
		String fileName = null;
		
		try
		{
			File f = new File(filePath);
			if(f.isFile())
			{
				fileName = f.getName();
			}
			
		} catch (Exception e)
		{
			// do nothing
		}
		
		return fileName;
	}
	

	public static String getRelativeFilePath(String originalFilePath, String newRelativePath)
	{
		String fileName= Application.getFileName(originalFilePath);
		return Application.getRelativeFilePath(originalFilePath, newRelativePath, fileName);
		
	}
	
	public static String getRelativeFilePath(String originalFilePath, String newRelativePath, String newFileName)
	{
		String directoryPath = Application.getDirectoryPath(originalFilePath);
		
		File d = new File(directoryPath);
		File f = null;
		
		if(newRelativePath == null)
		{
			f = new File(d, newFileName);
			
		} else
		{
			f = new File(d, newRelativePath + System.getProperty("file.separator") + newFileName);
		}
		
		return f.getPath();
		
	}

	
	public static void moveFile(String sourceFilePath, String targetFilePath, boolean replaceExisting, boolean assertTargetDirectoryExists)
	{
		File sf = new File(sourceFilePath);
		File tf = new File(targetFilePath);
		
		if(Application.fileExists(targetFilePath) && replaceExisting)
		{
			if(replaceExisting)
			{
				tf.delete();
			} else
			{
				throw new IllegalStateException("The target file \"" + targetFilePath + "\" already exists.");
			}
		}
		
		if(assertTargetDirectoryExists)
		{
			Application.assertDirectoryExists(targetFilePath);
		}
		
		sf.renameTo(tf);
		
	}
	
	public static void assertDirectoryExists(String targetPath)
	{
		String directoryPath = Application.getDirectoryPath(targetPath);
		
		if(Application.directoryExists(directoryPath))
		{
			return;
			
		} else
		{
			File d = new File(directoryPath);			
			d.mkdirs();
		}
	}
	    
	
	public static String getFileContents(String filePath) throws IOException 
	{
	    BufferedReader reader = null;
	    String line = null;
	    StringBuilder stringBuilder = new StringBuilder();
	    	    
	    try
	    {
	    	reader = new BufferedReader( new FileReader (filePath));
	   		    
		    String ls = System.getProperty("line.separator");
	
		    while ((line = reader.readLine()) != null ) {
		        stringBuilder.append(line);
		        stringBuilder.append(ls);
		    }
		    
	    } finally
	    {
	    	if(reader != null)
	    	{
	    		reader.close();
	    	}
	    }

	    return stringBuilder.toString().trim();
	}
	
}
