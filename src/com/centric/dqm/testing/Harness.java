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
	
	
	public void perfomTests() throws Exception
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
				
		boolean includeOnlyActive = false;
		
		// if there are no filters then return all active scenarios
		if(ScenarioFilterList.size() == 0 && TagList.size() == 0)
		{
			includeOnlyActive = true;
		}
						
		List<Scenario> matchList = new ArrayList<Scenario>();
					
		boolean includeScenario;
		
		for(Scenario sc : this.Scenarios)
		{
			// reset the include flag
			includeScenario = false;
			
			if(includeOnlyActive == true && sc.activeFlag == true)
			{
				// add the scenario to the list
				matchList.add(sc);
				continue;
			}
		
			// include if the identifier is in the list
			if(ScenarioFilterList.contains(sc.identifier.toLowerCase()))
			{
				// add the scenario to the list
				matchList.add(sc);
				continue;
			}
			
			// include if one of the tags match
			if(includeScenario == false && DataUtils.listsIntersect(sc.Tags, TagList))
			{
				// add the scenario to the list
				matchList.add(sc);
				continue;
			}
						
		}
		
		return matchList;		
	}
	

}