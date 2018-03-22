package org.stackoverflow.data.collector.task.data;

import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.config.KafkaConfig;

public class DataManagerTaskInfo {

	private KafkaProducer<String, String> dataProducer;
	private KafkaConsumer<String, String> metaConsumer;
	private String metaTopicName;
	private String dataTopicName;

	Logger log = LoggerFactory.getLogger(this.getClass());

	public DataManagerTaskInfo(String metaTopicName, String postTopicName) {
		
		try {

			this.metaTopicName = metaTopicName;
			this.dataTopicName = postTopicName;
			dataProducer = new KafkaProducer<>(KafkaConfig.getProducerConfig());
			log.info("Data Producer created.");
			Map<String, Object> config = KafkaConfig.getConsumerConfig();
			config.put(ConsumerConfig.GROUP_ID_CONFIG, "post-manager");
			metaConsumer = new KafkaConsumer<>(config);
			log.info("Meta Consumer created.");
			metaConsumer.subscribe(Arrays.asList(metaTopicName));
			log.info("Meta Consumer subscrbed for topic :: {}", metaTopicName);

		} catch (Exception e) {
			log.error("Exception occured while creating data manager task information.");
			throw e;
		}
	}

	public KafkaProducer<String, String> getDataProducer() {
		return dataProducer;
	}

	public void setDataProducer(KafkaProducer<String, String> dataProducer) {
		this.dataProducer = dataProducer;
	}

	public KafkaConsumer<String, String> getMetaConsumer() {
		return metaConsumer;
	}

	public void setMetaConsumer(KafkaConsumer<String, String> metaConsumer) {
		this.metaConsumer = metaConsumer;
	}

	public String getMetaTopicName() {
		return metaTopicName;
	}

	public void setMetaTopicName(String metaTopicName) {
		this.metaTopicName = metaTopicName;
	}

	public String getDataTopicName() {
		return dataTopicName;
	}

	public void setDataTopicName(String dataTopicName) {
		this.dataTopicName = dataTopicName;
	}
	
	

}
