package org.stackoverflow.data.collector.task.data;

import java.time.LocalDateTime;
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
import org.stackoverflow.data.collector.rest.client.HTTPRestClient;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PostManagerTask implements Runnable{

	private KafkaProducer<String, String> postProducer;
	private KafkaConsumer<String, String> postMetaConsumer;
	private String metaTopicName;
	private String postTopicName;
	
	Logger log = LoggerFactory.getLogger(this.getClass());
	
	public PostManagerTask(PostManagerTaskInfo managerTaskInfo) {
		
		try {
			
			this.metaTopicName = managerTaskInfo.getMetaTopicName();
			this.postTopicName = managerTaskInfo.getPostTopicName();
			postProducer = managerTaskInfo.getPostProducer();
			postMetaConsumer = managerTaskInfo.getPostMetaConsumer();	
			
		}
		catch (Exception e) {
			log.error("Exception occured while initializing the Post Meta Manager.");
			throw e;
		}

	}
	
	@Override
	public void run() {
		
		try {
			log.info("==================== Post manager task starts here. time : {} ==============================",LocalDateTime.now());
			ConsumerRecords<String, String> records = postMetaConsumer.poll(1000);
			postMetaConsumer.commitSync();
			log.info("Offset is synced with kafka topic.");
			log.info("{} of records consumed.",records.count());
			int size = 1;
			for(ConsumerRecord<String, String> record : records) {
				if(size > 1) {
					log.info("sleeping for another 40 seconds.");
					Thread.sleep(40*1000);					
				}
				log.info("Post Meta record details. offset :: {}, record :: {}", record.offset(), record.value());
				PostMetaModel model = PostMetaModelMapper.mapToPostMetaModel(record.value());
				fetchAndSendData(model);
				size++;
				
			}
			log.info("==================== Post manager task ends here. time : {} ==============================",LocalDateTime.now());
		}
		catch (Exception e) {
			log.error("Exception occured while managing the Post Meta Topic Producer and consumer. Exception :: ", e);
			
		}
	}
	
	private JsonNode parseResult (String result) throws Exception{
		JsonNode items = null;
		try {
			
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode node = mapper.readValue(result, ObjectNode.class);
			if(node.has("items")) {
				items = node.get("items");
			}
			
		}
		catch (Exception e) {
			log.error("Exception occured while parsing the json. json :: {}",result);
			throw e;
		}
		return items;
	}
	
	private String constructJSON(JsonNode items, PostMetaModel model) throws Exception {
		
		String json = null;
		try {
			
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode rootNode = mapper.createObjectNode();
			rootNode.set("items", items);
			rootNode.put("page", model.getPageNumber());
			rootNode.put("time", new Date().getTime());
			json = mapper.writeValueAsString(rootNode);
			
		}
		catch (Exception e) {
			log.info("Exception occured while constructing the new json ");
			throw e;
		}
		return json;
		
	}
	
	private void fetchAndSendData(PostMetaModel model) {
		
		try {
			
			String url = "https://api.stackexchange.com/2.2/posts?page=" + model.getPageNumber() + "&pagesize=100&fromdate=1451606400&order=asc&sort=creation&site=stackoverflow&key=2mhhH5LwC1aR7LN*uf4zGA((";
			log.info("Constructed URL :: {}",url);
			
			String result = HTTPRestClient.makeGetRequest(url);
			
			JsonNode items = parseResult(result);
			
			String json = constructJSON(items, model);
			
			log.info("Constructed JSON after attaching meta data. json :: {}", json);
			
			postProducer.send(new ProducerRecord<String, String>(postTopicName, json));
			
			log.info("published data to topic :: {}",postTopicName);
		}
		catch (Exception e) {
			log.error("Exception occured while fetching and sedning the data for page number :: {}", model);
		}
	}	

}
