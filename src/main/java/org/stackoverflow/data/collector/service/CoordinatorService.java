package org.stackoverflow.data.collector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.lock.DistributedLock;

public class CoordinatorService {
	
	private static final CoordinatorService COORDINATOR_SERVICE = new CoordinatorService();
	
	Logger log = LoggerFactory.getLogger(CoordinatorService.class);
	
	public static CoordinatorService getInstance() {
		return COORDINATOR_SERVICE;
	}
	
	public void start() {
		
		
		try {
			
			log.info("=========================== Cordinator Service started. ==============================================");
			log.info("Trying to acquire the distributed lock.");
			DistributedLock.acquireLock();
			if(DistributedLock.hasLock()) {
				MetaService.getInstance().start();
				DataService.getInstance().start();
			}
			
			log.info("=========================== Cordinator Service end here. ==============================================");
			
		}
		catch (Exception e) {
			log.error("Exception occured while starting the coordinator service ",e);
		}
	}

}
