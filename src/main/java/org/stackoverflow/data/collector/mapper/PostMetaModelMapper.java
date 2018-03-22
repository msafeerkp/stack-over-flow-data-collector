package org.stackoverflow.data.collector.mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stackoverflow.data.collector.model.MetaModel;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PostMetaModelMapper {
	
	private static Logger log = LoggerFactory.getLogger(PostMetaModelMapper.class);
	
	public static MetaModel mapToPostMetaModel(String metaModelString) throws Exception{
		
		log.debug("String to model convertion started . time :: {}",LocalDateTime.now());
		ObjectMapper objectMapper = new ObjectMapper();
		MetaModel model = null;
		try {
			model = objectMapper.readValue(metaModelString, MetaModel.class);
		}
		catch (Exception e) {
			log.error("Exception occured while convertion json to object.");
			throw e;
		}
		log.debug("String to model convertion completed . time :: {}",LocalDateTime.now());
		return model;
		
	}
	
	public static String mapPostMetaModelToValue(MetaModel model) throws Exception{
		
		log.debug("Model to String convertion started . time :: {}",LocalDateTime.now());
		ObjectMapper objectMapper = new ObjectMapper();
		String postMetaJSON = null;
		try {
			postMetaJSON = objectMapper.writeValueAsString(model);
		}
		catch (Exception e) {
			log.error("Exception occured while convertion json to object.");
			throw e;
		}
		log.debug("Model to String convertion completed . time :: {}",LocalDateTime.now());
		return postMetaJSON;
		
	}

}
