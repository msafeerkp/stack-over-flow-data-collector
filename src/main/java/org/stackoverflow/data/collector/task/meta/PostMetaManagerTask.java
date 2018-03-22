package org.stackoverflow.data.collector.task.meta;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.config.KafkaConfig;
import org.stackoverflow.data.collector.mapper.PostMetaModelMapper;
import org.stackoverflow.data.collector.model.MetaModel;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class PostMetaManagerTask implements Runnable{

	private KafkaProducer<String, String> metaProducer;
	private KafkaConsumer<String, String> metaConsumer;
	private String metaTopicName;
	private String[] metaTypes= {"ANSWER", "QUESTION", "COMMENT", "TAG", "USER"};
	
	Logger log = LoggerFactory.getLogger(this.getClass());
	
	public PostMetaManagerTask(String metaTopicName) {
		
		try {
			
			this.metaTopicName = metaTopicName;
			metaProducer = new KafkaProducer<>(KafkaConfig.getProducerConfig());
			log.info("Meta Producer created.");
			Map<String, Object> config = KafkaConfig.getConsumerConfig();
			config.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName() + " _" + this.hashCode());
			metaConsumer = new KafkaConsumer<>(config);
			log.info("Meta Consumer created.");		
			metaConsumer.subscribe(Arrays.asList(metaTopicName));
			log.info("Meta Consumer subscrbed for topic :: {}",metaTopicName);
			metaConsumer.poll(1000);
			log.info("performed intial poll to complet the partition assignment with meta consumer object.");
			
		}
		catch (Exception e) {
			log.error("Exception occured while initializing the Meta Manager.");
			throw e;
		}

	}
	
	@Override
	public void run() {
		
		try {
			log.info("==================== Meta manager task starts here. time : {} ==============================",LocalDateTime.now());
			
			Set<TopicPartition> partitions = metaConsumer.assignment();
			log.info("Partition allocated is :: {}",partitions);
			if(!partitions.isEmpty()) {
				metaConsumer.seekToEnd(partitions);
				log.info("Meta Consumer moves the head to the end.");
				TopicPartition metaTopicPartition = partitions.iterator().next();
				Long offset = metaConsumer.position(metaTopicPartition);
				log.info("Current end offset details. Offset :: {}", offset);
				if(offset == 0) {
					publishInitialRecord();
				}
				else {
					PublishNewRecord(metaTopicPartition, offset);
				}	
			}
			
			log.info("==================== Meta manager task ends here. time : {} ==============================",LocalDateTime.now());
			
		}
		catch (Exception e) {
			log.error("Exception occured while managing the Meta Topic Producer and consumer. Exception :: ", e);
			
		}
		
	}

	private void PublishNewRecord(TopicPartition metaTopicPartition, Long offset) throws Exception {
		
		try {
			
			metaConsumer.seek(metaTopicPartition, (offset-1));
			log.info("moving the head to the last record");
			ConsumerRecords<String, String> records = metaConsumer.poll(1000);
			log.info("Last record is fetched");
			if(records.count() == 1) {
				ConsumerRecord<String, String> record = records.iterator().next();
				log.info("Last record Value :: {}, offset position :: {}",record.value(),record.offset());
				boolean insertFlag = false;
				MetaModel model = PostMetaModelMapper.mapToPostMetaModel(record.value());
				int currentDayCount = Integer.parseInt(CollectorConfigUtil.getProperties().getProperty("collector.post.meta.day.count"));
				if(model.getDayCount() < currentDayCount) {
					model.setDayCount(model.getDayCount() + 1);
					insertFlag = true;
				}
				Instant modelInstant = Instant.ofEpochMilli(model.getTime());
				LocalDateTime modelDate = LocalDateTime.ofInstant(modelInstant,ZoneOffset.UTC);
				LocalDateTime currentDate = LocalDateTime.now(Clock.systemUTC());
				if(modelDate.getDayOfMonth() != currentDate.getDayOfMonth()) {
					log.info("Day Passed. reset the day count to 1.");
					model.setDayCount(1);
					insertFlag = true;
				}
				model.setTime(new Date().getTime());
				model.setPageNumber(model.getPageNumber() + 1);
				log.info("New record is ready to publish. record :: {}",model.toString());
				
				if(insertFlag) {
					for(String metaType : metaTypes) {
						model.setMetaType(metaType);
						metaProducer.send(new ProducerRecord<String, String>(metaTopicName, PostMetaModelMapper.mapPostMetaModelToValue(model)));
						log.info("New record is published details :: {} ",model.toString());
					}
					
				}
				
			}
			else {
				log.info("Last record could not recoverd.");
				publishInitialRecord();
			}
		}
		catch (Exception e) {
			log.info("Excption occured while publishing the new Post Meta Record");
			throw e;
		}
		
	}

	private void publishInitialRecord() throws Exception {
		
		try {
			
			for(String metaType : metaTypes) {
				MetaModel model = new MetaModel();
				model.setDayCount(1);
				model.setPageNumber(1);
				model.setTime(new Date().getTime());
				model.setMetaType(metaType);
				log.info("No record found. publishing the initial record. record :: {}", model.toString());
				metaProducer.send(new ProducerRecord<String, String>(metaTopicName, PostMetaModelMapper.mapPostMetaModelToValue(model)));
			}
			
			
			
		}
		catch (Exception e) {
			log.info("Exception occured while publishing the Intial records.");
			throw e;
		}
		
		
	}
	
	

}
