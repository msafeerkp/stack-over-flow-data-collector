package org.stackoverflow.data.collector.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.lock.DistributedLock;
import org.stackoverflow.data.collector.task.meta.PostMetaManagerTask;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class PostMetaService {
	
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private static final PostMetaService POST_META_SERVICE = new PostMetaService();
	
	public static PostMetaService getInstance() {
		return POST_META_SERVICE;
	}

	public void start() {
		
		log.info(" ============================== Post Meta Service started ===================================");
		
		try {
			
			DistributedLock.acquireLock();
			if(DistributedLock.hasLock()) {
				String postMetaTopicName = CollectorConfigUtil.getProperties().getProperty("collector.post.meta.topic.name");
				Thread postMetaProducer = new Thread(new PostMetaManagerTask(postMetaTopicName));
				ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
				executorService.scheduleAtFixedRate(postMetaProducer, 60, 5, TimeUnit.SECONDS);
			}
			
		}
		catch (Exception e) {
			log.error("Exception occured while starting the Post Meta Service. Exception :: ",e);
		}
		
		log.info(" ============================== Post Meta Service ends here. ===================================");
		
	}

}
