package org.stackoverflow.data.collector.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.lock.DistributedLock;
import org.stackoverflow.data.collector.task.data.DataManagerTask;
import org.stackoverflow.data.collector.task.data.DataManagerTaskInfo;
import org.stackoverflow.data.collector.task.meta.PostMetaManagerTask;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class DataService {

	private Logger log = LoggerFactory.getLogger(this.getClass());
	private static final DataService DATA_SERVICE = new DataService();
	
	public static DataService getInstance() {
		return DATA_SERVICE;
	}
	
	public void start() {
		
		log.info(" ============================== Data Service started ===================================");
		
		try {
				String dataTopicName = CollectorConfigUtil.getProperties().getProperty("collector.post.topic.name");
				String metaTopicName = CollectorConfigUtil.getProperties().getProperty("collector.post.meta.topic.name");
				DataManagerTaskInfo managerTaskInfo = new DataManagerTaskInfo(metaTopicName, dataTopicName);
				Thread dataProducer = new Thread(new DataManagerTask(managerTaskInfo));
				ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
				executorService.scheduleAtFixedRate(dataProducer, 0, 40, TimeUnit.SECONDS);
				
				/*DataManagerTaskInfo managerTaskInfo = new DataManagerTaskInfo(metaTopicName, dataTopicName);
				Thread dataProducer = new Thread(new DataManagerTask(managerTaskInfo));
				dataProducer.setName(" Task 1");
				dataProducer.start();
				int epoch = 1;
				int taskNumber = 2;
				while(true) {
					log.info("Current Epoch is :: {}", epoch++);
					if(dataProducer.isAlive()) {
						log.info("Previous task is not completed. sleeping for another 60 seconds. Thread Name :: {} ",dataProducer.getName());
						Thread.sleep(1 * 60 * 1000);
					}
					else {
						log.info("Current Task Token :: {}", taskNumber);
						
						dataProducer = new Thread(new DataManagerTask(managerTaskInfo));
						dataProducer.setName(" Task "+taskNumber);
						dataProducer.start();
						log.info("Created new Thread and started. Thread Name :: {}", dataProducer.getName());
						taskNumber++;
					}
					
				}*/
			
		}
		catch (Exception e) {
			log.error("Exception occured while starting the Post Service. Exception :: ",e);
		}
		
		log.info(" ============================== Post Meta Service ends here. ===================================");
		
	}
	
}
