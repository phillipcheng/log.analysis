package etl.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;


public class DBUtil {
	public static final Logger logger = Logger.getLogger(DBUtil.class);

	public static final String key_db_driver="db.driver";
	public static final String key_db_url="db.url";
	public static final String key_db_user="db.user";
	public static final String key_db_password="db.password";
	public static final String key_db_loginTimeout="db.loginTimeout";

	//db
	public static String normalizeDBFieldName(String fn){
		return fn.replaceAll("[ .-]", "_");
	}

	public static String guessDBType(String value){
		int len = value.length();
		try {
			Float.parseFloat(value);
			return String.format("numeric(%d,%d)", 15,5);
		}catch(Exception e){
			return String.format("varchar(%d)", Math.max(20, 2*len));
		}
	}

	public static String genCreateTableSql(List<String> fieldNameList, List<String> fieldTypeList, String tn, String dbschema){
		StringBuffer tablesql = new StringBuffer();
		for (int i=0; i<fieldNameList.size(); i++){
			fieldNameList.set(i,normalizeDBFieldName(fieldNameList.get(i)));
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
			String name = normalizeDBFieldName(fieldNameList.get(i));
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
			fieldNameList.set(i,normalizeDBFieldName(fieldNameList.get(i)));
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
			fieldNameList.set(i,normalizeDBFieldName(fieldNameList.get(i)));
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

	public static int executeSqls(List<String> sqls, PropertiesConfiguration pc){
		Connection conn = null;
		int rowsUpdated = 0;
		try {
			conn = getConnection(pc);
			if (conn!=null){
				for (String sql:sqls){
					logger.info(sql);
					Statement stmt = conn.createStatement();
					try {
						boolean result = stmt.execute(sql);
						if (!result){
							rowsUpdated = stmt.getUpdateCount();
							logger.info(String.format("%d rows accepted.", rowsUpdated));
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
		logger.info("Rows Updated:"+rowsUpdated);
		return rowsUpdated;
	}
	public static boolean checkTableExists(String sql, PropertiesConfiguration pc){
		Connection conn = null;
		try {
			conn = getConnection(pc);
			if (conn!=null){
				Statement stmt = conn.createStatement();
				try {
					ResultSet result = stmt.executeQuery(sql);
					if (result.next()){
						logger.info(String.format("Table exists "));
						return true;
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
		}catch(Exception e){
			logger.error("", e);
		}finally{
			try{
				conn.close();
			}catch(Exception e){
				logger.error("", e);
			}
		}
		return false;
	}
	
	public static ArrayList<String> checkCsv(String sql, PropertiesConfiguration pc, int startIndex, int endIndex,String columnSeparator){
        Connection conn = null;
        ArrayList<String> dbCsvData=new ArrayList<String>();
        try {
               conn = getConnection(pc);
               if (conn!=null){
                     Statement stmt = conn.createStatement();
                     try {
                            ResultSet result = stmt.executeQuery(sql);
                            ResultSetMetaData rsmd = result.getMetaData();
                            int endColIdx = rsmd.getColumnCount();
                            int startColIdx=1;
                           
                            String colValue=null;
                            if(startIndex!=0){
                                   startColIdx=startIndex;
                            }
                            if(endIndex!=0){
                                   endColIdx=endIndex;
                            }
                            while(result.next()){
                                   colValue="";
                                   for (int i =startColIdx; i <=endColIdx; i++)
                                   {
                                     if(i == endColIdx){
                                            colValue=colValue+result.getString(i);
                                            continue;
                                     }
                                       colValue=colValue+result.getString(i)+columnSeparator;
                                   }
                                   dbCsvData.add(colValue);
                            } 
                            if(!dbCsvData.isEmpty())
                            {
                                   return dbCsvData;
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
        }catch(Exception e){
               logger.error("", e);
        }finally{
               try{
                     conn.close();
               }catch(Exception e){
                     logger.error("", e);
               }
        }

        return dbCsvData;
	}
}
