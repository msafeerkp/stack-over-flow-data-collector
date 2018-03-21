package org.stackoverflow.data.collector.task.data;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.config.KafkaConfig;

public class PostManagerTaskInfo {

	private KafkaProducer<String, String> postProducer;
	private KafkaConsumer<String, String> postMetaConsumer;
	private String metaTopicName;
	private String postTopicName;

	Logger log = LoggerFactory.getLogger(this.getClass());

	public PostManagerTaskInfo(String metaTopicName, String postTopicName) {
		
		try {

			this.metaTopicName = metaTopicName;
			this.postTopicName = postTopicName;
			postProducer = new KafkaProducer<>(KafkaConfig.getProducerConfig());
			log.info("Post Producer created.");
			Map<String, Object> config = KafkaConfig.getConsumerConfig();
			config.put(ConsumerConfig.GROUP_ID_CONFIG, "post-manager");
			postMetaConsumer = new KafkaConsumer<>(config);
			log.info("Post Meta Consumer created.");
			postMetaConsumer.subscribe(Arrays.asList(metaTopicName));
			log.info("Post Meta Consumer subscrbed for topic :: {}", metaTopicName);

		} catch (Exception e) {
			log.error("Exception occured while initializing the Post Meta Manager.");
			throw e;
		}
	}

	public KafkaProducer<String, String> getPostProducer() {
		return postProducer;
	}

	public void setPostProducer(KafkaProducer<String, String> postProducer) {
		this.postProducer = postProducer;
	}

	public KafkaConsumer<String, String> getPostMetaConsumer() {
		return postMetaConsumer;
	}

	public void setPostMetaConsumer(KafkaConsumer<String, String> postMetaConsumer) {
		this.postMetaConsumer = postMetaConsumer;
	}

	public String getMetaTopicName() {
		return metaTopicName;
	}

	public void setMetaTopicName(String metaTopicName) {
		this.metaTopicName = metaTopicName;
	}

	public String getPostTopicName() {
		return postTopicName;
	}

	public void setPostTopicName(String postTopicName) {
		this.postTopicName = postTopicName;
	}
	
	

}
