package hpe.mtc;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;


public class LogicSchema {
	public static final Logger logger = Logger.getLogger(LogicSchema.class);
	
	private Map<String, List<String>> schemas = null;
	// table-name to table definition (list of attributes)
	
	public LogicSchema(){
		schemas = new HashMap<String, List<String>>();
	}
	
	@JsonIgnore
	public List<String> getAttributes(String tableName){
		return schemas.get(tableName);
	}
	
	public void updateOrAddAttributes(String tableName, List<String> attributes){
		schemas.put(tableName, attributes);
	}
	
	public static final String charset="utf8";
	public static LogicSchema fromJsonString(String json){
		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		try {
			Object t = mapper.readValue(json, LogicSchema.class);
			return (LogicSchema) t;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	public static LogicSchema fromFile(String file){
		try {
			byte[] encoded = Files.readAllBytes(Paths.get(file));
			String content = new String(encoded, charset);
			return fromJsonString(content);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public static String toJsonString(LogicSchema ls){
		ObjectMapper om = new ObjectMapper();
		om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		ObjectWriter ow = om.writer().with(new MinimalPrettyPrinter());
		try {
			String json = ow.writeValueAsString(ls);
			return json;
		} catch (JsonProcessingException e) {
			logger.error("",e );
			return null;
		}
	}
	
	public static void toFile(String file, LogicSchema ls){
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

	public Map<String, List<String>> getSchemas() {
		return schemas;
	}

	public void setSchemas(Map<String, List<String>> schemas) {
		this.schemas = schemas;
	}
}
