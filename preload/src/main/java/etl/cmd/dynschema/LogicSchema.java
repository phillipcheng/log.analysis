package etl.cmd.dynschema;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import etl.util.DBUtil;
import etl.util.Util;


public class LogicSchema {
	public static final Logger logger = Logger.getLogger(LogicSchema.class);
	
	private Map<String, List<String>> attrNameMap = null; //table-name to list of attribute names mapping
	private Map<String, List<String>> objParamMap = null; //table-name to table object parameter names mapping
	
	public LogicSchema(){
		attrNameMap = new HashMap<String, List<String>>();
		setObjParamMap(new HashMap<String, List<String>>());
	}
	
	@JsonIgnore
	public List<String> getAttrNames(String tableName){
		return attrNameMap.get(tableName);
	}
	
	public boolean hasAttrNames(String tableName){
		return attrNameMap.containsKey(tableName);
	}
	
	public void addAttributes(String tableName, List<String> attributes){
		List<String> originAttr = attrNameMap.get(tableName);
		if (originAttr==null){
			attrNameMap.put(tableName, attributes);
		}else{
			originAttr.addAll(attributes);
			attrNameMap.put(tableName, originAttr);
		}
	}
	@JsonIgnore
	public List<String> getObjParams(String tableName){
		return objParamMap.get(tableName);
	}
	
	public void addObjParams(String tableName, List<String> objParams){
		objParamMap.put(tableName, objParams);
	}
	
	public LogicSchema clone(){
		String str = Util.toJsonString(this);
		return (LogicSchema) Util.fromJsonString(str, LogicSchema.class);
	}

	public Map<String, List<String>> getSchemas() {
		return attrNameMap;
	}
	public void setSchemas(Map<String, List<String>> schemas) {
		this.attrNameMap = schemas;
	}
	public Map<String, List<String>> getObjParamMap() {
		return objParamMap;
	}
	public void setObjParamMap(Map<String, List<String>> objParamMap) {
		this.objParamMap = objParamMap;
	}
}
