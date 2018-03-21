package org.stackoverflow.data.collector.rest.client;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class HTTPRestClient {
	
	private static final CloseableHttpClient httpClient = HttpClients.createDefault();
	
	public static String makeGetRequest(String uri) throws Exception{
		
		HttpGet get = new HttpGet(uri);
		String responseBondy;
		
		try (
				CloseableHttpResponse httpResponse = httpClient.execute(get) ; 
				InputStream content = httpResponse.getEntity().getContent()
			) {
			responseBondy = IOUtils.toString(content,StandardCharsets.UTF_8.name());
		}
		catch (Exception exception) {
			// need to manage the log
			throw exception;
		}
		
		return responseBondy;
		

		
	}
	
	

}
