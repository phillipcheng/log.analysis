package bdap.util;

import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class JsonUtil {
	public static final Logger logger = LogManager.getLogger(JsonUtil.class);
	//json serialization
	public static final String charset="utf8";
	
	public static <T> T fromJsonString(String json, Class<T> clazz){
		return fromJsonString(json, clazz, false);
	}
	
	public static String toJsonString(Object ls){
		return toJsonString(ls, false);
	}
	
	public static <T> T fromJsonString(String json, Class<T> clazz, boolean useDefaultTyping){
		ObjectMapper mapper = new ObjectMapper();
		if (useDefaultTyping){
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		}
		mapper.setSerializationInclusion(Include.NON_NULL);
		try {
			return mapper.readValue(json, clazz);
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}

	public static <T> T fromJsonString(String json, String attrName, Class<T> clazz){
		return fromJsonString(json, attrName, clazz, false);
	}
	
	public static <T> T fromJsonString(String json, String attrName, Class<T> clazz, boolean useDefaultTyping){
		ObjectMapper mapper = new ObjectMapper();
		if (useDefaultTyping){
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		}
		mapper.setSerializationInclusion(Include.NON_NULL);
		try {
			JsonNode root = mapper.readTree(json);
			JsonNode attr = root;
			if (attrName != null) {
				String[] attrNames = attrName.split("\\.");
				for (String a: attrNames)
					attr = attr.get(a);
			}
			return mapper.treeToValue(attr, clazz);
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	public static String toJsonString(Object ls, boolean useDefaultTyping){
		ObjectMapper mapper = new ObjectMapper();
		if (useDefaultTyping){
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		}
		mapper.setSerializationInclusion(Include.NON_NULL);
		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		try {
			String json = ow.writeValueAsString(ls);
			return json;
		} catch (JsonProcessingException e) {
			logger.error("",e );
			return null;
		}
	}
	
	//from use default typing to not using default typing
	public static void migrateJson(String infile, String outfile, Class clazz){
		java.nio.file.Path path = java.nio.file.FileSystems.getDefault().getPath(infile);
		java.nio.file.Path outpath = java.nio.file.FileSystems.getDefault().getPath(outfile);
		try {
			String contents = new String(Files.readAllBytes(path));
			Object obj = fromJsonString(contents, clazz, true);
			if (obj!=null){
				String newContents = toJsonString(obj, false);
				Files.write(outpath, newContents.getBytes());
			}else{
				logger.error(String.format("error reading infile:%s", infile));
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public static void toLocalJsonFile(String file, Object ls){
		PrintWriter out = null;
		try{
			out = new PrintWriter(file, charset);
			out.println(toJsonString(ls));
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (out!=null)
				out.close();
		}
	}
	
	public static Object fromLocalJsonFile(String file, Class clazz){
		try {
			java.nio.file.Path path = java.nio.file.FileSystems.getDefault().getPath(file);
			if (!Files.exists(path)) {
				URL fileURL = ClassLoader.getSystemResource(file);
				if (fileURL != null) {
					path = Paths.get(fileURL.toURI());
					String contents = new String(Files.readAllBytes(path), Charset.forName("utf8"));
					return fromJsonString(contents, clazz);
				} else {
					logger.debug("File does not exist: {}", file);
					return null;
				}
			} else {
				String contents = new String(Files.readAllBytes(path), Charset.forName("utf8"));
				return fromJsonString(contents, clazz);
			}
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
}
