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
import org.stackoverflow.data.collector.model.MetaModel;
import org.stackoverflow.data.collector.rest.client.HTTPRestClient;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DataManagerTask implements Runnable{

	private KafkaProducer<String, String> dataProducer;
	private KafkaConsumer<String, String> metaConsumer;
	private String metaTopicName;
	private String dataTopicName;
	
	Logger log = LoggerFactory.getLogger(this.getClass());
	
	public DataManagerTask(DataManagerTaskInfo managerTaskInfo) {
		
		try {
			
			this.metaTopicName = managerTaskInfo.getMetaTopicName();
			this.dataTopicName = managerTaskInfo.getDataTopicName();
			dataProducer = managerTaskInfo.getDataProducer();
			metaConsumer = managerTaskInfo.getMetaConsumer();	
			
		}
		catch (Exception e) {
			log.error("Exception occured while initializing the Data Manager.");
			throw e;
		}

	}
	
	@Override
	public void run() {
		
		try {
			log.info("==================== Data manager task starts here. time : {} ==============================",LocalDateTime.now());
			ConsumerRecords<String, String> records = metaConsumer.poll(1000);
			log.info("{} of records consumed.",records.count());
			int size = 1;
			for(ConsumerRecord<String, String> record : records) {
				if(size > 1) {
					log.info("sleeping for another 40 seconds.");
					Thread.sleep(40*1000);					
				}
				log.info("Meta record details. offset :: {}, record :: {}", record.offset(), record.value());
				MetaModel model = PostMetaModelMapper.mapToPostMetaModel(record.value());
				fetchAndSendData(model);
				size++;
			}
			metaConsumer.commitSync();
			log.info("Operation completed. manual offset commit with kafka topic.");
			log.info("==================== Data manager task ends here. time : {} ==============================",LocalDateTime.now());
		}
		catch (Exception e) {
			log.error("Exception occured while performing the data collection from stack over flow Rest API. Exception :: ", e);
			
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
	
	private String constructJSON(JsonNode items, MetaModel model) throws Exception {
		
		String json = null;
		try {
			
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode rootNode = mapper.createObjectNode();
			rootNode.set("items", items);
			rootNode.put("page", model.getPageNumber());
			rootNode.put("time", new Date().getTime());
			rootNode.put("metaType", model.getMetaType());
			json = mapper.writeValueAsString(rootNode);
			
		}
		catch (Exception e) {
			log.info("Exception occured while constructing the new json ");
			throw e;
		}
		return json;
		
	}
	
	private String getAnswersURL(long pageNumber) {
		
		return "https://api.stackexchange.com/2.2/answers?page=" + pageNumber + "&pagesize=100&fromdate=1451606400&order=asc&sort=creation&site=stackoverflow&filter=!)rFTNQ*AJvS9TcirNNeR&key=2mhhH5LwC1aR7LN*uf4zGA((";		
	}
	
	private String getQuestionsURL(long pageNumber) {
		
		return "https://api.stackexchange.com/2.2/questions?page=" + pageNumber + "&pagesize=100&fromdate=1451606400&order=asc&sort=creation&site=stackoverflow&filter=!0V-ZwUF)BGHC98o7f12qctJ0D&key=2mhhH5LwC1aR7LN*uf4zGA((";		
	}
	
	private String getCommentsURL(long pageNumber) {
		
		return "https://api.stackexchange.com/2.2/comments?page=" + pageNumber + "&pagesize=100&fromdate=1451606400&order=asc&sort=creation&site=stackoverflow&filter=!-*jbN.wzETsw&key=2mhhH5LwC1aR7LN*uf4zGA((";		
	}
	
	private String getTagsURL(long pageNumber) {
		
		return "https://api.stackexchange.com/2.2/tags?page=" + pageNumber + "&pagesize=100&order=asc&sort=activity&site=stackoverflow&filter=!9Z(-wqiNh&key=2mhhH5LwC1aR7LN*uf4zGA((";		
	}
	
	private String getUsersURL(long pageNumber) {
		
		return "https://api.stackexchange.com/2.2/users?page=" + pageNumber + "&pagesize=100&order=asc&sort=creation&site=stackoverflow&filter=!-*jbN*IioeFP&key=2mhhH5LwC1aR7LN*uf4zGA((";		
	}
	
	private String getURL(MetaModel metaModel) throws Exception{
		
		String result = null;
		try {
			switch (metaModel.getMetaType()) {
			case "ANSWER":
				result = getAnswersURL(metaModel.getPageNumber());
				break;
			case "QUESTION":
				result = getQuestionsURL(metaModel.getPageNumber());
				break;
			case "COMMENT":
				result = getCommentsURL(metaModel.getPageNumber());
				break;
			case "TAG":
				result = getTagsURL(metaModel.getPageNumber());
				break;
			case "USER":
				result = getUsersURL(metaModel.getPageNumber());
				break;
			}
		}
		catch (Exception e) {
			log.error("Exception occured while constructing the URL.");
			throw e;
		}
		
		return result;
	}
	
	
	private void fetchAndSendData(MetaModel model) throws Exception {
		
		try {
			
			//String url = "https://api.stackexchange.com/2.2/posts?page=" + model.getPageNumber() + "&pagesize=100&fromdate=1451606400&order=asc&sort=creation&site=stackoverflow&key=2mhhH5LwC1aR7LN*uf4zGA((";
			String url = getURL(model);
			
			log.info("Constructed URL :: {}",url);
			
			String result = HTTPRestClient.makeGetRequest(url);
			
			JsonNode items = parseResult(result);
			
			String json = constructJSON(items, model);
			
			log.info("Constructed JSON after attaching meta data. json :: {}", json);
			
			dataProducer.send(new ProducerRecord<String, String>(dataTopicName, json));
			
			log.info("published data to topic :: {}",dataTopicName);
		}
		catch (Exception e) {
			log.error("Exception occured while fetching and sedning the data for page number :: {}", model);
			throw e;
		}
	}	

}
