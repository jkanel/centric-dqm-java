package com.centric.dqm.testing;

import java.util.ArrayList;
import java.util.List;

import com.centric.dqm.Application;
import com.centric.dqm.data.DataUtils;

public class Harness {
	
	public List<Scenario> Scenarios = new ArrayList<Scenario>();
	
	public List<String> ScenarioFilterList;
	public List<String> TagList;
	
	public void addScenario(Scenario Scenario)
	{
		Scenarios.add(Scenario);
	}
	
	
	public void perfomTests()
	{
		List<Scenario> matchScenarioFilterList = this.getMatchScenarios(); 
		
		Application.logger.info("Identified (" + matchScenarioFilterList.size() + ") matching test(s).");
		
		for(Scenario sc : matchScenarioFilterList)
		{
			sc.performTest();
		}	
		
		
	}
	
	public List<Scenario> getMatchScenarios()
	{
		
		// if there are no filters then return all scenarios
		if(ScenarioFilterList.size() == 0 || TagList.size() == 0)
		{
			return this.Scenarios;
		}
						
		List<Scenario> matchList = new ArrayList<Scenario>();
					
		boolean includeScenario;
		
		for(Scenario sc : this.Scenarios)
		{
			// reset the include flag
			includeScenario = false;
		
			// include if the identifier is in the list
			if(ScenarioFilterList.contains(sc.identifier))
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