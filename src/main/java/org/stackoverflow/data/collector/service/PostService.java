package org.stackoverflow.data.collector.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.lock.DistributedLock;
import org.stackoverflow.data.collector.task.data.PostManagerTask;
import org.stackoverflow.data.collector.task.data.PostManagerTaskInfo;
import org.stackoverflow.data.collector.task.meta.PostMetaManagerTask;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class PostService {

	private Logger log = LoggerFactory.getLogger(this.getClass());
	private static final PostService POST_SERVICE = new PostService();
	
	public static PostService getInstance() {
		return POST_SERVICE;
	}
	
	public void start() {
		
		log.info(" ============================== Post Service started ===================================");
		
		try {
				String postTopicName = CollectorConfigUtil.getProperties().getProperty("collector.post.topic.name");
				String postMetaTopicName = CollectorConfigUtil.getProperties().getProperty("collector.post.meta.topic.name");
				//Thread postProducer = new Thread(new PostManagerTask(postMetaTopicName,postTopicName));
				//ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
				//executorService.scheduleAtFixedRate(postProducer, 0, 40, TimeUnit.SECONDS);
				PostManagerTaskInfo managerTaskInfo = new PostManagerTaskInfo(postMetaTopicName, postTopicName);
				Thread postProducer = new Thread(new PostManagerTask(managerTaskInfo));
				postProducer.setName(" Task 1");
				postProducer.start();
				int epoch = 1;
				int taskNumber = 2;
				while(true) {
					log.info("Current Epoch is :: {}", epoch++);
					if(postProducer.isAlive()) {
						log.info("Previous task is not completed. sleeping for another 60 seconds. Thread Name :: {} ",postProducer.getName());
						Thread.sleep(1 * 60 * 1000);
					}
					else {
						log.info("Current Task Token :: {}", taskNumber);
						
						postProducer = new Thread(new PostManagerTask(managerTaskInfo));
						postProducer.setName(" Task "+taskNumber);
						postProducer.start();
						log.info("Created new Thread and started. Thread Name :: {}", postProducer.getName());
						taskNumber++;
					}
					
				}
			
		}
		catch (Exception e) {
			log.error("Exception occured while starting the Post Service. Exception :: ",e);
		}
		
		log.info(" ============================== Post Meta Service ends here. ===================================");
		
	}
	
}
