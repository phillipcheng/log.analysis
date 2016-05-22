package hpe.mtc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

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
	
	public static String genCreateTableSql(List<String> fieldNameList, List<String> fieldTypeList, String tn, String dbschema){
		StringBuffer tablesql = new StringBuffer();
		for (int i=0; i<fieldNameList.size(); i++){
			fieldNameList.set(i,normalizeFieldName(fieldNameList.get(i)));
		}
		//gen table sql
		tablesql.append(String.format("create table if not exists %s.%s(\n", dbschema, tn));
		for (int i=0; i<fieldNameList.size(); i++){
			String name = fieldNameList.get(i);
			String type = fieldTypeList.get(i);
			tablesql.append(String.format("%s %s", name, type));
			if (i<fieldNameList.size()-1){
				tablesql.append(",");
			}
		}
		tablesql.append(");\n");
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
	
	//with the input file as %s
	public static String genCopySql(List<String> fieldNameList, String tn, String csvFolder, String dbschema){
		StringBuffer copysql = new StringBuffer();
		for (int i=0; i<fieldNameList.size(); i++){
			fieldNameList.set(i,normalizeFieldName(fieldNameList.get(i)));
		}
		//gen table sql
		copysql.append(String.format("copy %s.%s(\n", dbschema, tn));
		for (int i=0; i<fieldNameList.size(); i++){
			String name = fieldNameList.get(i);
			copysql.append(String.format("%s enclosed by '\"'", name));
			if (i<fieldNameList.size()-1){
				copysql.append(",");
			}
		}
		csvFolder = csvFolder.replace("\\", "/");
		copysql.append(") from local '%s' delimiter ',' direct;\n");
		return copysql.toString();
	}
	
	public static String getCsv(List<String> csv){
		StringBuffer sb = new StringBuffer();
		for (String v:csv){
			if (v!=null){
				v = "\"" + v + "\"";//always enclosed by "\""
				sb.append(v).append(",");
			}else{
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
}
