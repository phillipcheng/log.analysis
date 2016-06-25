package etl.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;


public class Util {
	public static final Logger logger = Logger.getLogger(Util.class);
	
	public static final String key_db_driver="db.driver";
	public static final String key_db_url="db.url";
	public static final String key_db_user="db.user";
	public static final String key_db_password="db.password";
	public static final String key_db_loginTimeout="db.loginTimeout";
	
	
	public static PropertiesConfiguration getPropertiesConfig(String conf){
		PropertiesConfiguration pc = null;
		try {
			URL url = Thread.currentThread().getContextClassLoader().getResource(conf);
			pc = new PropertiesConfiguration(url);
		} catch (ConfigurationException e) {
			File f = new File(conf);
			try {
				pc = new PropertiesConfiguration(f);
			}catch(Exception e1){
				logger.error("", e1);
			}
		}
		return pc;
	}
	
	public static PropertiesConfiguration getPropertiesConfigFromDfs(FileSystem fs, String conf){
		BufferedReader br = null;
		try {
			Path ip = new Path(conf);
	        br=new BufferedReader(new InputStreamReader(fs.open(ip)));
	        PropertiesConfiguration pc = new PropertiesConfiguration();
	        pc.load(br);
	        return pc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}finally{
			if (br!=null){
				try{
					br.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	//k1=v1,k2=v2 =>{{k1,v1},{k2,v2}}
	public static TreeMap<String, String> parseMapParams(String params){
		TreeMap<String, String> paramsMap = new TreeMap<String, String>();
		if (params==null){
			return paramsMap;
		}
		String[] strParams = params.split(",");
		for (String strParam:strParams){
			String[] kv = strParam.split("=");
			if (kv.length<2){
				logger.error(String.format("wrong param format: %s", params));
			}else{
				paramsMap.put(kv[0].trim(), kv[1].trim());
			}
		}
		return paramsMap;
	}
	
	public static String normalizeFieldName(String fn){
		return fn.replaceAll("[ .-]", "_");
	}
	
	public static String getCsv(List<String> csv){
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<csv.size(); i++){
			String v = csv.get(i);
			if (v!=null){
				v = "\"" + v + "\"";//always enclosed by "\""
				sb.append(v);
			}
			if (i<csv.size()-1){
				sb.append(",");
			}
		}
		sb.append("\n");
		return sb.toString();
	}
	
	public static String guessType(String value){
		int len = value.length();
		try {
			Float.parseFloat(value);
			return String.format("numeric(%d,%d)", 15,5);
		}catch(Exception e){
			return String.format("varchar(%d)", Math.max(20, 2*len));
		}
	}
	
	//json serialization
	public static final String charset="utf8";
	public static Object fromJsonString(String json, Class clazz){
		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		try {
			Object t = mapper.readValue(json, clazz);
			return t;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}

	public static String toJsonString(Object ls){
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
	
	public static void toFile(String file, Object ls){
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
	
	public static void toDfsFile(FileSystem fs, String file, Object ls){
		BufferedWriter out = null;
		try{
			out = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(file))));
			out.write(toJsonString(ls));
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (out!=null){
				try{
					out.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static Object fromDfsFile(FileSystem fs, String file, Class clazz){
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			IOUtils.copy(fs.open(new Path(file)), baos);
			String content = baos.toString(charset);
			return fromJsonString(content, clazz);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public static List<String> stringsFromDfsFile(FileSystem fs, String file){
		BufferedReader in = null;
		List<String> sl = new ArrayList<String>();
		try {
			in = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
			String s =null;
			while ((s=in.readLine())!=null){
				sl.add(s);
			}
			return sl;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	//db
	public static String genCreateTableSql(List<String> fieldNameList, List<String> fieldTypeList, String tn, String dbschema){
		StringBuffer tablesql = new StringBuffer();
		for (int i=0; i<fieldNameList.size(); i++){
			fieldNameList.set(i,normalizeFieldName(fieldNameList.get(i)));
		}
		//gen table sql
		tablesql.append(String.format("create table if not exists %s.%s(", dbschema, tn));
		for (int i=0; i<fieldNameList.size(); i++){
			String name = fieldNameList.get(i);
			String type = fieldTypeList.get(i);
			tablesql.append(String.format("%s %s", name, type));
			if (i<fieldNameList.size()-1){
				tablesql.append(",");
			}
		}
		tablesql.append(");");
		return tablesql.toString();
	}
	
	public static List<String> genUpdateTableSql(List<String> fieldNameList, List<String> fieldTypeList, String tn, String dbschema){
		List<String> updateSqls = new ArrayList<String>();
		for (int i=0; i<fieldNameList.size(); i++){
			String name = normalizeFieldName(fieldNameList.get(i));
			updateSqls.add(String.format("alter table %s.%s add column %s %s;\n", dbschema, tn, name, fieldTypeList.get(i)));
		}
		return updateSqls;
	}
	
	public static String genDropTableSql(String tn, String dbschema){
		StringBuffer tablesql = new StringBuffer();
		tablesql.append(String.format("drop table %s.%s;\n", dbschema, tn));
		return tablesql.toString();
	}
	
	public static String genTruncTableSql(String tn, String dbschema){
		StringBuffer tablesql = new StringBuffer();
		tablesql.append(String.format("truncate table %s.%s;\n", dbschema, tn));
		return tablesql.toString();
	}
	
	public static String genCopyLocalSql(List<String> fieldNameList, String tn, String dbschema, String csvFileName){
		StringBuffer copysql = new StringBuffer();
		for (int i=0; i<fieldNameList.size(); i++){
			fieldNameList.set(i,normalizeFieldName(fieldNameList.get(i)));
		}
		//gen table sql
		copysql.append(String.format("copy %s.%s(", dbschema, tn));
		for (int i=0; i<fieldNameList.size(); i++){
			String name = fieldNameList.get(i);
			copysql.append(String.format("%s enclosed by '\"'", name));
			if (i<fieldNameList.size()-1){
				copysql.append(",");
			}
		}
		copysql.append(String.format(") from local '%s' delimiter ',' direct;", csvFileName));
		return copysql.toString();
	}
	
	public static String genCopyHdfsSql(List<String> fieldNameList, String tn, String dbschema, 
			String rootWebHdfs, String csvFileName, String username){
		StringBuffer copysql = new StringBuffer();
		for (int i=0; i<fieldNameList.size(); i++){
			fieldNameList.set(i,normalizeFieldName(fieldNameList.get(i)));
		}
		//gen table sql
		copysql.append(String.format("copy %s.%s(", dbschema, tn));
		for (int i=0; i<fieldNameList.size(); i++){
			String name = fieldNameList.get(i);
			copysql.append(String.format("%s enclosed by '\"'", name));
			if (i<fieldNameList.size()-1){
				copysql.append(",");
			}
		}
		copysql.append(String.format(") SOURCE Hdfs(url='%s%s',username='%s') delimiter ',';", rootWebHdfs, csvFileName, username));
		return copysql.toString();
	}
	
	private static Connection getConnection(PropertiesConfiguration pc){
		Connection conn = null;
		try { 
        	Class.forName(pc.getString(key_db_driver)); 
        } catch (ClassNotFoundException e) {
        	logger.error("", e);
        }
        Properties myProp = new Properties();
        myProp.put("user", pc.getString(key_db_user));
        myProp.put("password", pc.getString(key_db_password));
        myProp.put("loginTimeout", pc.getString(key_db_loginTimeout));
        try {
            conn = DriverManager.getConnection(pc.getString(key_db_url), myProp);
            logger.debug("connected!");
        }catch(Exception e){
            logger.error("", e);
        }
        return conn;
	}
	
	public static void executeSqls(List<String> sqls, PropertiesConfiguration pc){
        Connection conn = null;
        try {
            conn = getConnection(pc);
            if (conn!=null){
	            for (String sql:sqls){
	            	Statement stmt = conn.createStatement();
	            	try {
	            		boolean result = stmt.execute(sql);
	            		if (!result){
	            			logger.info(String.format("%d rows accepted.", stmt.getUpdateCount()));
	            		}
	            		SQLWarning warning = stmt.getWarnings();
	            		while (warning != null){
	            		   logger.info(warning.getMessage());
	            		   warning = warning.getNextWarning();
	            		}
	            	}catch(Exception e){
	            		logger.error(e.getMessage());
	            	}finally{
	            		stmt.close();
	            	}
	            }
            }
        }catch(Exception e){
            logger.error("", e);
        }finally{
        	try{
        		conn.close();
        	}catch(Exception e){
        		logger.error("", e);
        	}
        }
	}
	
	//files
	public static void zipFiles(ZipOutputStream zos, String inputFolder, List<String> inputFileNames) throws IOException {
		byte[] buffer = new byte[1024];
		for(String file : inputFileNames){
    		ZipEntry ze= new ZipEntry(file);
        	zos.putNextEntry(ze);
        	FileInputStream in = new FileInputStream(inputFolder + file);
        	int len;
        	while ((len = in.read(buffer)) > 0) {
        		zos.write(buffer, 0, len);
        	}
        	in.close();
        	zos.closeEntry();
    	}
	}
	
	public static void deleteFiles(String folder, List<String> fileNames) throws IOException {
		for (String fileName : fileNames){
			File file = new File(folder + fileName);
			file.delete();
		}
	}
	
	public static void writeFile(String fileName, List<String> contents){
		BufferedWriter osw = null;
		try {
			osw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
			for (String line:contents){
				osw.write(line);
				osw.write("\n");
			}
		}catch(Exception e){
			logger.error("",e);
		}finally{
			if (osw!=null){
				try {
					osw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static void writeDfsFile(FileSystem fs, String fileName, List<String> contents){
		BufferedWriter osw = null;
		try {
			osw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(fileName))));
			for (String line:contents){
				osw.write(line);
				osw.write("\n");
			}
		}catch(Exception e){
			logger.error("",e);
		}finally{
			if (osw!=null){
				try {
					osw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static List<String> listDfsFile(FileSystem fs, String folder){
		List<String> files = new ArrayList<String>();
		try {
			FileStatus[] fslist = fs.listStatus(new Path(folder));
			for (FileStatus f:fslist){
				files.add(f.getPath().getName());
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return files;
	}
}
