package com.centric.dqm.testing;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.centric.dqm.Application;
import com.centric.dqm.Configuration;
import com.centric.dqm.data.DataUtils;
import com.centric.dqm.data.HarnessWriter;
import com.centric.dqm.data.IConnection;

public class Query {	

	
	public static final String COMMAND_PARAM_CURRENT_TIME = "<<CURRENT_TIME>>";
	public static final String COMMAND_PARAM_CURRENT_DATE = "<<CURRENT_DATE>>";
	public static final String COMMAND_PARAM_CURRENT_YEAR = "<<CURRENT_YEAR>>";
	public static final String COMMAND_PARAM_MODULUS = "<<MODULUS>>";
	public static final String COMMAND_PARAM_MODULARITY = "<<MODULARITY>>";
	public static final String IMPORT_DIRECTIVE = "@import";
	
	public Scenario parent;
	public String scenarioMode; 
	
	public String errorMessage;
	public Integer errorNumber;
	
	public String commandText;	
	
	public Exception queryException = null;
	
	public Query(){}
		
	public ResultSet execute(IConnection connection)
	{		
		return this.execute(connection, 1, 0);
	}
	
	public ResultSet execute (IConnection connection, Integer modulus, Integer modularity)
	{
		
		try
		{
			
			// optionally resolve the query from a file
			resolveCommandFromFile();
			
			String parameterizeCommandText = Query.parameterizeCommandText(this.commandText, modulus, modularity);		
			return connection.executeCommandWithResult(parameterizeCommandText);
				
		} catch(Exception e)
		{
			this.queryException = e;
			Application.logger.error(Application.getExceptionStackTrace(e));
			
			return null;
		}
		
	}
	
	protected void resolveCommandFromFile() throws IOException, URISyntaxException
	{	
		if(this.commandText != null)
		{
			// determine if length is greater than a file path length
			if(this.commandText.length() > 260)
			{
				// exit the method, command is not a file path
				return;
			}
			
			String filePath = this.commandText;
			String filePathRelative = Application.getJarFullyQualifiedPath(this.commandText);
			String contents = null;			
			
			// determine if the current command is a valid file path
			if(Application.fileExists(filePathRelative))
			{				
				contents = Application.getFileContents(filePathRelative);
				
				// set the file path for future use
				filePath = filePathRelative;
								
			} else if (Application.fileExists(filePath))
			{
				contents = Application.getFileContents(filePath);
				
			} else
			{
				// exit the method, assumes the command is a valid query
				return;
			}
			
			// if the contents start with the import directive
			// import the query text into the table.
			if(contents.startsWith(Query.IMPORT_DIRECTIVE))
			{
				this.commandText = contents.substring(Query.IMPORT_DIRECTIVE.length()).trim();
				HarnessWriter.updateQueryCommand(Configuration.Connection, this.parent, this.scenarioMode, this.commandText);
								
				String targetFilePath = Application.getRelativeFilePath(filePath, "imported", Application.getFileName(filePath));
				Application.moveFile(filePath, targetFilePath, true,true);
				
				
			} else
			{
				this.commandText = contents;
			}
								
		}
		
	}
	
	public static String parameterizeCommandText(String commandText, Integer modulus, Integer modularity)
	{
		
		Date currentDate = new Date();
		
		SimpleDateFormat SdfDatetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat SdfDate = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat SdfYear = new SimpleDateFormat("yyyy");

		String value = commandText;
		
		value = value.replace(COMMAND_PARAM_CURRENT_TIME, DataUtils.delimitSQLString(SdfDatetime.format(currentDate)));		
		value = value.replace(COMMAND_PARAM_CURRENT_TIME,  DataUtils.delimitSQLString(SdfDate.format(currentDate)));
		value = value.replace(COMMAND_PARAM_CURRENT_YEAR,  SdfYear.format(currentDate));
		
		if(modulus == null || modularity == null || modulus <= 0 || modularity < 0)
		{
			value = value.replace(COMMAND_PARAM_MODULUS, "1");
			value = value.replace(COMMAND_PARAM_MODULARITY,  "0");
			
		} else
		{
			value = value.replace(COMMAND_PARAM_MODULUS, String.valueOf(modulus));
			value = value.replace(COMMAND_PARAM_MODULARITY,  String.valueOf(modularity));
		}
		
		return value;
		
	}
	
}
