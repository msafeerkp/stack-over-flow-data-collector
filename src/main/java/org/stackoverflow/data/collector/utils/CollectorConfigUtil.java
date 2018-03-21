package org.stackoverflow.data.collector.utils;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectorConfigUtil {
	
	static Logger log = LoggerFactory.getLogger(CollectorConfigUtil.class);
	static Properties properties;
	
	static {
		try {
			log.info("===== Loading the properties file data into memmory =====");
			InputStream inputStream = CollectorConfigUtil.class.getClassLoader().getResourceAsStream("collector.properties");
			properties = new Properties();
			properties.load(inputStream);
			log.info("===== Loading the properties file data into memmory ======");
		}
		catch (Exception e) {
			log.error("Exception Occured while loading the porperties file into memmory.");
		}

	}
	
	public static Properties getProperties() {
		return properties;
	}

}
