package org.stackoverflow.data.collector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorService {
	
	private static final CoordinatorService COORDINATOR_SERVICE = new CoordinatorService();
	
	Logger log = LoggerFactory.getLogger(CoordinatorService.class);
	
	public static CoordinatorService getInstance() {
		return COORDINATOR_SERVICE;
	}
	
	public void start() {
		
		
		try {
			
			log.info("=========================== Cordinator Service started. ==============================================");
			PostMetaService.getInstance().start();
			PostService.getInstance().start();
			log.info("=========================== Cordinator Service end here. ==============================================");
			
		}
		catch (Exception e) {
			log.error("Exception occured while starting the coordinator service ",e);
		}
	}

}
