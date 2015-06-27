package com.centric.dqm.testing;

import java.util.ArrayList;
import java.util.List;

import com.centric.dqm.data.DataUtils;

public class Harness {
	
	public List<Scenario> Scenarios = new ArrayList<Scenario>();
	
	public List<String> IdentifierList;
	public List<String> TagList;
	
	public void addScenario(Scenario Scenario)
	{
		Scenarios.add(Scenario);
	}
	
	public void perfomTests()
	{
		for(Scenario sc : this.Scenarios)
		{
			sc.performTest();
		}		
	}
	
	public void perfomTests(String scenarioIdentifiers, String tags)
	{
		
		if(scenarioIdentifiers.length()>0 || tags.length()>0)
		{
			// get Scenario sublist		
			for(Scenario sc : this.getScenarios(scenarioIdentifiers, tags))
			{
				sc.performTest();
			}			
		}
		else
		{
			// test all scenarios		
			for(Scenario sc : this.Scenarios)
			{
				sc.performTest();
			}	
		}
	}
	
	public List<Scenario> getScenarios(String scenarioIdentifiers, String tags)
	{
		
		// if there are no filters then return all scenarios
		if(scenarioIdentifiers == null && tags == null)
		{
			return this.Scenarios;
		}
						
		List<Scenario> matchList = new ArrayList<Scenario>();
		
		// generate lists
		List<String> IdentifierList = DataUtils.getListFromString(scenarioIdentifiers);
		List<String> TagList = DataUtils.getListFromString(tags);
				
		boolean includeScenario;
		
		for(Scenario sc : this.Scenarios)
		{
			// reset the include flag
			includeScenario = false;
		
			// include if the identifier is in the list
			if(IdentifierList.contains(sc.identifier))
			{
				includeScenario = true;
			}
			
			// include if one of the tags match
			if(includeScenario == false && DataUtils.listsIntersect(sc.Tags, TagList))
			{
				includeScenario = true;
			}
			
			// add to the match list
			if(includeScenario == true)
			{
				matchList.add(sc);
			}			
		}
		
		return matchList;		
	}
	

}