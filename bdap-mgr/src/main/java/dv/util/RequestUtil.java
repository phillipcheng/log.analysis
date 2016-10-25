package dv.util;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;


public class RequestUtil {
	public static final Logger logger = LogManager.getLogger(RequestUtil.class);
	
	private static RestTemplate getRestTemplate(String proxyHost, int proxyPort) {
		RestTemplate restTemplate = new RestTemplate();
		if (StringUtils.isNotEmpty(proxyHost)) {
			// set proxy for request
			SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
			InetSocketAddress address = new InetSocketAddress(proxyHost, proxyPort);
			Proxy proxy = new Proxy(Proxy.Type.HTTP, address);
			factory.setProxy(proxy);
			restTemplate.setRequestFactory(factory);
		}
		return restTemplate;
	}

	public static String post(String url, Map<String, String> headMap, String body) {
		return post(url, null, 0, headMap, body);
	}
	
	public static String post(String url,String proxyHost, int proxyPort, Map<String, String> headMap,
			String body) {
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType.parseMediaType("application/xml; charset=UTF-8");
		headers.setContentType(type);
		headers.add("Accept", MediaType.APPLICATION_XML_VALUE.toString());
		if (headMap != null) {
			for (String key : headMap.keySet()) {
				headers.add(key, headMap.get(key));
			}
		}
		RestTemplate restTemplate = getRestTemplate(proxyHost,  proxyPort);
		HttpEntity<String> formEntity = new HttpEntity<String>(body, headers);
		try {
			String result = restTemplate.postForObject(url, formEntity,
					String.class);
			return result;
		}catch(RestClientException e){
			logger.error(String.format("url:%s\n, header:%s\n, body:%s", url, headMap, body), e);
			return null;
		}
	}

	public static String get(String url, String proxyHost, int proxyPort, Map<String, String> headMap) {
		HttpHeaders headers = new HttpHeaders();
		if (headMap != null) {
			for (String key : headMap.keySet()) {
				headers.add(key, headMap.get(key));
			}
		}
		HttpEntity<String> request = new HttpEntity<String>(headers);
		RestTemplate restTemplate = getRestTemplate(proxyHost, proxyPort);

		ResponseEntity<String> response = restTemplate.exchange(url,
				HttpMethod.GET, request, String.class);
		String result = response.getBody();
		return result;
	}

	public static String put(String url, String proxyHost, int proxyPort, Map<String, String> headMap,
			String body) {
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType
				.parseMediaType("application/xml; charset=UTF-8");
		headers.setContentType(type);
		headers.add("Accept", MediaType.APPLICATION_XML_VALUE.toString());
		if (headMap != null) {
			for (String key : headMap.keySet()) {
				headers.add(key, headMap.get(key));
			}
		}
		HttpEntity<String> formEntity = new HttpEntity<String>(body, headers);
		RestTemplate restTemplate = getRestTemplate(proxyHost, proxyPort);
		restTemplate.put(url, formEntity);
		return "";
	}

	public static String delete(String url, String proxyHost, int proxyPort, Map<String, String> headMap,
			String body) {
		HttpHeaders headers = new HttpHeaders();
		MediaType type = MediaType
				.parseMediaType("application/xml; charset=UTF-8");
		headers.setContentType(type);
		headers.add("Accept", MediaType.APPLICATION_XML_VALUE.toString());
		if (headMap != null) {
			for (String key : headMap.keySet()) {
				headers.add(key, headMap.get(key));
			}
		}
		HttpEntity<String> formEntity = new HttpEntity<String>(body, headers);
		RestTemplate restTemplate = getRestTemplate(proxyHost, proxyPort);
		restTemplate.delete(url);
		return "";
	}

}
