package org.stackoverflow.data.collector.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.lock.DistributedLock;
import org.stackoverflow.data.collector.task.meta.PostMetaManagerTask;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class MetaService {
	
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private static final MetaService POST_META_SERVICE = new MetaService();
	
	public static MetaService getInstance() {
		return POST_META_SERVICE;
	}

	public void start() {
		
		log.info(" ============================== Meta Service started ===================================");
		
		try {
			
			DistributedLock.acquireLock();
			if(DistributedLock.hasLock()) {
				String metaTopicName = CollectorConfigUtil.getProperties().getProperty("collector.post.meta.topic.name");
				Thread metaProducer = new Thread(new PostMetaManagerTask(metaTopicName));
				ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
				executorService.scheduleAtFixedRate(metaProducer, 40, 5*40, TimeUnit.SECONDS);
			}
			
		}
		catch (Exception e) {
			log.error("Exception occured while starting the Meta Service. Exception :: ",e);
		}
		
		log.info(" ============================== Meta Service ends here. ===================================");
		
	}

}
