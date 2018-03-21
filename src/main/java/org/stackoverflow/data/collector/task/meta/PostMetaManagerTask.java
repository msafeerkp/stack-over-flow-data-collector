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
import org.stackoverflow.data.collector.model.PostMetaModel;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class PostMetaManagerTask implements Runnable{

	private KafkaProducer<String, String> postMetaProducer;
	private KafkaConsumer<String, String> postMetaConsumer;
	private String metaTopicName;
	
	Logger log = LoggerFactory.getLogger(this.getClass());
	
	public PostMetaManagerTask(String metaTopicName) {
		
		try {
			
			this.metaTopicName = metaTopicName;
			postMetaProducer = new KafkaProducer<>(KafkaConfig.getProducerConfig());
			log.info("Post Meta Producer created.");
			Map<String, Object> config = KafkaConfig.getConsumerConfig();
			config.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName() + " _" + this.hashCode());
			postMetaConsumer = new KafkaConsumer<>(config);
			log.info("Post Meta Consumer created.");		
			postMetaConsumer.subscribe(Arrays.asList(metaTopicName));
			log.info("Post Meta Consumer subscrbed for topic :: {}",metaTopicName);
			postMetaConsumer.poll(1000);
			log.info("performed intial poll to complet the partition assignment with meta consumer object.");
			
		}
		catch (Exception e) {
			log.error("Exception occured while initializing the Post Meta Manager.");
			throw e;
		}

	}
	
	@Override
	public void run() {
		
		try {
			log.info("==================== Post meta manager task starts here. time : {} ==============================",LocalDateTime.now());
			
			Set<TopicPartition> partitions = postMetaConsumer.assignment();
			log.info("Partition allocated is :: {}",partitions);
			if(!partitions.isEmpty()) {
				postMetaConsumer.seekToEnd(partitions);
				log.info("Post Meta Consumer moves the head to the end.");
				TopicPartition metaTopicPartition = partitions.iterator().next();
				Long offset = postMetaConsumer.position(metaTopicPartition);
				log.info("Current end offset details. Offset :: {}", offset);
				if(offset == 0) {
					publishInitialRecord();
				}
				else {
					PublishNewRecord(metaTopicPartition, offset);
				}	
			}
			
			log.info("==================== Post meta manager task ends here. time : {} ==============================",LocalDateTime.now());
			
		}
		catch (Exception e) {
			log.error("Exception occured while managing the Post Meta Topic Producer and consumer. Exception :: ", e);
			
		}
		
	}

	private void PublishNewRecord(TopicPartition metaTopicPartition, Long offset) throws Exception {
		
		try {
			
			postMetaConsumer.seek(metaTopicPartition, (offset-1));
			log.info("moving the head to the last record");
			ConsumerRecords<String, String> records = postMetaConsumer.poll(1000);
			log.info("Last record is fetched");
			if(records.count() == 1) {
				ConsumerRecord<String, String> record = records.iterator().next();
				log.info("Last record Value :: {}, offset position :: {}",record.value(),record.offset());
				boolean insertFlag = false;
				PostMetaModel model = PostMetaModelMapper.mapToPostMetaModel(record.value());
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
					postMetaProducer.send(new ProducerRecord<String, String>(metaTopicName, PostMetaModelMapper.mapPostMetaModelToValue(model)));
					log.info("New record is published ");
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
			
			PostMetaModel model = new PostMetaModel();
			model.setDayCount(1);
			model.setPageNumber(1);
			model.setTime(new Date().getTime());
			log.info("No record found. publishing the initial record. record :: {}", model.toString());
			postMetaProducer.send(new ProducerRecord<String, String>(metaTopicName, PostMetaModelMapper.mapPostMetaModelToValue(model)));
			
		}
		catch (Exception e) {
			log.info("Exception occured while publishing the Intial record.");
			throw e;
		}
		
		
	}
	
	

}
