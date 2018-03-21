package org.stackoverflow.data.collector.startup;

import org.stackoverflow.data.collector.service.CoordinatorService;

public class Start {
	
	public static void main(String[] args)  {
	
		CoordinatorService.getInstance().start();
		
	}

}
