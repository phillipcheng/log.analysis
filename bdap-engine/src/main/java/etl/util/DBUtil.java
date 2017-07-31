package etl.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.cmd.transform.TableIdx;
import etl.engine.ETLCmd;
import etl.engine.LogicSchema;
import etl.engine.types.DBType;

import org.apache.commons.configuration.PropertiesConfiguration;


public class DBUtil {
	public static final Logger logger = LogManager.getLogger(DBUtil.class);

	public static final String key_db_type="db.type";
	public static final String key_db_driver="db.driver";
	public static final String key_db_url="db.url";
	public static final String key_db_user="db.user";
	public static final String key_db_password="db.password";
	public static final String key_db_loginTimeout="db.loginTimeout";

	public static final String var_prefix="$";//idx variable prefix
	
	private static String normalizeDBFieldName(String fn){
		return fn.replaceAll("[ .-]", "_");
	}
	
	public static List<String> getFieldNames(TableIdx ti, LogicSchema ls){
		List<String> ret = new ArrayList<String>();
		List<Integer> indexes = IdxRange.getIdxInRange(ti.getIdxR(), ti.getColNum());
		if (ETLCmd.SINGLE_TABLE.equals(ti.getTableName())){
			for (int idx:indexes){
				ret.add(ETLCmd.COLUMN_PREFIX+idx);
			}
		}else{
			List<String> an = ls.getAttrNames(ti.getTableName());
			for (int idx:indexes){
				ret.add(an.get(idx));
			}
		}
		return ret;
	}
	//expand the idx variables in the sql
	public static String updateVar(String sql, Map<String, TableIdx> idxMap, LogicSchema ls){
		for (String vn: idxMap.keySet()){
			String varName = var_prefix + vn;
			TableIdx ti = idxMap.get(vn);
			int i = sql.lastIndexOf(varName);
			while(i >= 0) {
				//at i we found varName
				int begin = i;//include
				int end = begin + varName.length();//exclude
				int replaceBegin=begin;
				int replaceEnd=end;
				String functionName=null;
				if (sql.length()>end && sql.charAt(end)==')'){//replace function
					replaceEnd++;
					begin--;//(
					char ch=sql.charAt(begin);
					while(ch!=' '){
						replaceBegin--;
						ch=sql.charAt(replaceBegin);
					}
					replaceBegin++;
					functionName = sql.substring(replaceBegin, begin);
				}
				List<String> fns = getFieldNames(ti, ls);
				StringBuffer sb = new StringBuffer();
				for (int j=0; j<fns.size(); j++){
					String fn = fns.get(j);
					if (functionName!=null){
						sb.append(String.format("%s(%s.%s)", functionName, ti.getTableName(), fn));
					}else{
						sb.append(String.format("%s.%s", ti.getTableName(), fn));
					}
					if (j<fns.size()-1){
						sb.append(",");
					}
				}
				//replace sql from replaceBegin to replaceEnd
				sql = sql.substring(0, replaceBegin) + sb.toString() + sql.substring(replaceEnd);
				logger.debug(String.format("sql:%s", sql));
			    i = sql.lastIndexOf(varName, i-1);
			}
		}
		return sql;
	}
	
	private static List<String> normalizeDBFieldNames(List<String> fieldNameList){
		List<String> retlist = new ArrayList<String>();
		for (String fn:fieldNameList){
			retlist.add(normalizeDBFieldName(fn));
		}
		return retlist;
	}

	public static FieldType guessDBType(String value){
		int len = value.length();
		try {
			Float.parseFloat(value);
			return new FieldType(VarType.NUMERIC, 15, 5);
		}catch(Exception e){
			return new FieldType(VarType.STRING,Math.max(20, 2*len));
		}
	}
	public static FieldType guessDBType(String name,String value){
		int len = value.length();
		try {
			if((name.endsWith("time") || name.endsWith("Time")) && value.length() > 8){
				return new FieldType(VarType.TIMESTAMP, 0, 0);
			}else{
				Float.parseFloat(value);
				return new FieldType(VarType.NUMERIC, Math.max(20, 2*len), 4);
			}
		}catch(Exception e){
			return new FieldType(VarType.STRING,Math.max(20, 2*len));
		}
	}

	public static String genCreateTableSql(List<String> fieldNameList, List<FieldType> fieldTypeList, 
			String tn, String dbschema, DBType dbtype, StoreFormat sf){
		List<String> fnl = normalizeDBFieldNames(fieldNameList);
		StringBuffer tablesql = new StringBuffer();
		//gen table sql
		tablesql.append(String.format("create table if not exists %s.%s(", dbschema, tn));
		for (int i=0; i<fnl.size(); i++){
			String name = fnl.get(i);
			FieldType type = fieldTypeList.get(i);
			tablesql.append(String.format("%s %s", name, type.toSql(dbtype)));
			if (i<fnl.size()-1){
				tablesql.append(",");
			}
		}
		tablesql.append(");");
		if (DBType.HIVE == dbtype){
			if (StoreFormat.parquet==sf){
				tablesql.append(" STORED AS PARQUET");
			}else if (StoreFormat.text==sf){
				tablesql.append(" ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"");
			}else{
				logger.error(String.format("format not supported:%s", sf));
			}
		}
		return tablesql.toString();
	}

	public static List<String> genUpdateTableSql(List<String> fieldNameList, List<FieldType> fieldTypeList, 
			String tn, String dbschema, DBType dbtype){
		List<String> updateSqls = new ArrayList<String>();
		if (DBType.VERTICA == dbtype || DBType.NONE == dbtype){
			for (int i=0; i<fieldNameList.size(); i++){
				String name = normalizeDBFieldName(fieldNameList.get(i));
				updateSqls.add(String.format("alter table %s.%s add column %s %s;", dbschema, tn, 
						name, fieldTypeList.get(i).toSql(dbtype)));
			}
		}else{
			StringBuffer sb = new StringBuffer();
			sb.append(String.format("alter table %s.%s add columns (", dbschema, tn));
			for (int i=0; i<fieldNameList.size(); i++){
				String name = normalizeDBFieldName(fieldNameList.get(i));
				sb.append(String.format("%s %s", name, fieldTypeList.get(i).toSql(dbtype)));
				if (i<fieldNameList.size()-1){
					sb.append(",");
				}
			}
			sb.append(")");
			updateSqls.add(sb.toString());
		}
		return updateSqls;
	}
	
	public static List<String> genDropFiedSql(List<String> fieldNameList, List<FieldType> fieldTypeList, 
			String tn, String dbschema, DBType dbtype){
		List<String> updateSqls = new ArrayList<String>();
		if (DBType.VERTICA == dbtype || DBType.NONE == dbtype){
			for (int i=0; i<fieldNameList.size(); i++){
				String name = normalizeDBFieldName(fieldNameList.get(i));
				updateSqls.add(String.format("alter table %s.%s drop column %s;", dbschema, tn, 
						name));
			}
		}else{
			StringBuffer sb = new StringBuffer();
			sb.append(String.format("alter table %s.%s drop columns (", dbschema, tn));
			for (int i=0; i<fieldNameList.size(); i++){
				String name = normalizeDBFieldName(fieldNameList.get(i));
				sb.append(String.format("%s", name));
				if (i<fieldNameList.size()-1){
					sb.append(",");
				}
			}
			sb.append(");");
			updateSqls.add(sb.toString());
		}
		return updateSqls;
	}

	public static String genDropTableSql(String tn, String dbschema){
		StringBuffer tablesql = new StringBuffer();
		tablesql.append(String.format("drop table %s.%s;", dbschema, tn));
		return tablesql.toString();
	}

	public static String genTruncTableSql(String tn, String dbschema){
		StringBuffer tablesql = new StringBuffer();
		tablesql.append(String.format("truncate table %s.%s;\n", dbschema, tn));
		return tablesql.toString();
	}

	//for external invocation
	public static String genCopyHdfsSql(String prefix, List<String> fieldNameList, String tn, String dbschema, 
			String rootWebHdfs, String csvFileName, String username, String dbType){
		DBType dbtype = DBType.fromValue(dbType);
		return genCopyHdfsSql(prefix, fieldNameList, tn, dbschema, rootWebHdfs, csvFileName, username, dbtype);
	}
	
	public static String genCopyHdfsSql(String prefix, List<String> fieldNameList, String tn, String dbschema, 
			String rootWebHdfs, String csvFileName, String username, DBType dbType){		
		return genCopyHdfsSql(prefix, fieldNameList, tn, dbschema, rootWebHdfs, csvFileName, username, dbType, ",", "\"", null, null);
	}
	
	public static String genCopyHdfsSql(String prefix, List<String> fieldNameList, String tn, String dbschema, 
			String rootWebHdfs, String csvFileName, String username, DBType dbType, String delimiter, String enclosedBy, String escapeChar, String recordTerminator){
		if (DBType.HIVE == dbType){
			return String.format("load data inpath '%s%s' into table %s.%s", rootWebHdfs, csvFileName, dbschema, tn);
		}else{
			StringBuilder copysql = new StringBuilder();
			List<String> fnl = normalizeDBFieldNames(fieldNameList);
			//gen table sql
			if (prefix==null){
				copysql.append(String.format("copy %s.%s(", dbschema, tn));
			}else{
				copysql.append(String.format("copy %s.%s(%s", dbschema, tn, prefix));
			}
			for (int i=0; i<fnl.size(); i++){
				String name = fnl.get(i);
				copysql.append(String.format("%s enclosed by '%s'", name, enclosedBy));
				if (i<fnl.size()-1){
					copysql.append(",");
				}
			}
			copysql.append(String.format(") SOURCE Hdfs(url='%s%s*',username='%s') delimiter '%s'", rootWebHdfs, csvFileName, username, delimiter));
			if(escapeChar!=null && !escapeChar.isEmpty()){
				copysql.append(String.format(" escape as '%s'", escapeChar));
			}
			if(recordTerminator!=null && !recordTerminator.isEmpty()){
				copysql.append(String.format(" record terminator '%s'", recordTerminator));
			}
			copysql.append(" ;");
			return copysql.toString();
		}
	}

	public static Connection getConnection(PropertiesConfiguration pc){
		String dbType = pc.getString(key_db_type);
		if (DBType.NONE.value().equals(dbType)){
			return null;
		}
		Connection conn = null;
		try { 
			Class.forName(pc.getString(key_db_driver)); 
		} catch (ClassNotFoundException e) {
			logger.error("", e);
		}
		Properties myProp = new Properties();
		myProp.put("user", pc.getString(key_db_user));
		myProp.put("password", pc.getString(key_db_password));
		myProp.put("loginTimeout", pc.getString(key_db_loginTimeout, "35"));
		try {
			conn = DriverManager.getConnection(pc.getString(key_db_url), myProp);
			logger.debug("connected!");
		}catch(Exception e){
			logger.error("", e);
		}
		return conn;
	}

	public static int executeSqls(List<String> sqls, PropertiesConfiguration pc) throws Exception{
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
							rowsUpdated += stmt.getUpdateCount();
							logger.info(String.format("%d rows accepted.", rowsUpdated));
						}
						SQLWarning warning = stmt.getWarnings();
						while (warning != null){
							logger.info(warning.getMessage());
							warning = warning.getNextWarning();
						}
					}finally{
						stmt.close();
					}
				}
			}
		}finally{
			try{
				if (conn!=null){
					conn.close();
				}
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
				if (conn!=null){
					conn.close();
				}
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
            	   if (conn!=null){
                     conn.close();
            	   }
               }catch(Exception e){
                     logger.error("", e);
               }
        }

        return dbCsvData;
	}
}
