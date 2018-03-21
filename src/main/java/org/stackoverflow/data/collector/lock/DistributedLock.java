package org.stackoverflow.data.collector.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;
import org.stackoverflow.data.collector.zookeeper.CuratorFrameworkUtil;

public class DistributedLock {
	
	private static Logger log = LoggerFactory.getLogger(DistributedLock.class);
	private static InterProcessMutex interProcessMutex;
	
	static {
		
		try {
			
			log.info("Creating the curator framework client.");
			CuratorFramework curatorFramework = CuratorFrameworkUtil.getClient();
			curatorFramework.start();
			log.info("curator framework client started.");
			interProcessMutex = new InterProcessMutex(curatorFramework, CollectorConfigUtil.getProperties().getProperty("collector.distributed.lock.path"));
			log.info("Distributed lock handler is created.");
			
		}
		catch (Exception e) {
			log.info("Exception occured while creating the lock handler exception details ::",e.getMessage());
		}
		
	}
	
	public static boolean acquireLock() throws Exception {
		
		try {
			log.info("Acquring the lock.");
			interProcessMutex.acquire();
			log.info("lock acquired.");
			
		} catch (Exception e) {
			log.info("Exception occured while acquiring the lock ");
			throw e;
		}
		return true;
	}
	public static boolean releaseLock() throws Exception {
		
		try {
			log.info("releasing the lock.");
			interProcessMutex.release();
			log.info("lock released.");
		} catch (Exception e) {
			
			log.info("Exception occured while releasing the lock ");
			throw e;
		}
		return true;
		
	}
	
	public static boolean hasLock() throws Exception{
		
		try {
			log.info("checking the lock existence");
			interProcessMutex.isAcquiredInThisProcess();
			log.info("lock exist in the current process");
		} catch (Exception e) {
			log.info("Exception occured while acquiring the lock ");
			throw e;
		}
		return true;
	}

}
