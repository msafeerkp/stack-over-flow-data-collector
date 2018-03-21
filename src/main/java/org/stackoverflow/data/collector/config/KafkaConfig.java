package org.stackoverflow.data.collector.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class KafkaConfig {
	
	private static Map<String, Object> getCommonConfig() {
		
		Map<String, Object> config = new HashMap<>();
		Properties collectorProps = CollectorConfigUtil.getProperties();
		config.put("bootstrap.servers", collectorProps.getProperty("collector.kafka.bootstrap.servers"));
		return config;
		
	}
	
	public static  Map<String, Object> getProducerConfig() {
		
		Map<String, Object> config = getCommonConfig();
		Properties collectorProps = CollectorConfigUtil.getProperties();
		
		config.put("acks", collectorProps.getProperty("collector.kafka.acks"));
		config.put("retries", collectorProps.getProperty("collector.kafka.retries"));
		config.put("batch.size", collectorProps.getProperty("collector.kafka.batch.size"));
		config.put("linger.ms", collectorProps.getProperty("collector.kafka.linger.ms"));
		config.put("buffer.memory", collectorProps.getProperty("collector.kafka.buffer.memory"));
		config.put("key.serializer", collectorProps.getProperty("collector.kafka.key.serializer"));
		config.put("value.serializer", collectorProps.getProperty("collector.kafka.value.serializer"));
		return config;
	
	}
	
	public static Map<String, Object> getConsumerConfig() {
		
		 Properties collectorProps = CollectorConfigUtil.getProperties();
		 Map<String, Object> config = getCommonConfig();
		// config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, collectorProps.getProperty("collector.kafka.enable.auto.commit"));
		// config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, collectorProps.getProperty("collector.kafka.auto.commit.interval.ms"));
		 config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, collectorProps.getProperty("collector.kafka.key.deserializer"));
		 config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, collectorProps.getProperty("collector.kafka.value.deserializer"));
		 config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		 return config;
	     
	}
	
	public static Map<String, Object> getAdminConfig() {
	     return getCommonConfig();
	}

	
}
