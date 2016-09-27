package etl.util;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import etl.engine.LogicSchema;

public class SchemaUtils {
	public static final Logger logger = LogManager.getLogger(SchemaUtils.class);
	
	public static FieldType getFieldType(int type, int size, int digits){
		if (Types.TIMESTAMP == type){
			return new FieldType(VarType.TIMESTAMP);
		}else if (Types.VARCHAR == type){
			return new FieldType(VarType.STRING, size);
		}else if (Types.NUMERIC == type){
			return new FieldType(VarType.NUMERIC, size, digits);
		}else if (Types.DECIMAL == type){
			return new FieldType(VarType.NUMERIC, size, digits);
		}else if (Types.BIGINT == type){
			return new FieldType(VarType.INT);
		}else if (Types.DATE == type){
			return new FieldType(VarType.DATE);
		}else{
			logger.error(String.format("not supported:%d", type));
			return null;
		}
	}
	
	public static List<String> genCreateSqlByLogicSchema(LogicSchema ls, String dbSchema, DBType dbtype){
		List<String> sqls = new ArrayList<String>();
		for (String tn: ls.getAttrNameMap().keySet()){
			List<String> attrNames = ls.getAttrNames(tn);
			List<FieldType> attrTypes = ls.getAttrTypes(tn);
			String sql = DBUtil.genCreateTableSql(attrNames, attrTypes, tn, dbSchema, dbtype);
			sqls.add(sql);
		}
		return sqls;
	}
	
	public static void genCreateSqls(String schemaFile, String outputSql, String dbSchema, DBType dbtype){
		try{
			LogicSchema ls = (LogicSchema) Util.fromLocalJsonFile(schemaFile, LogicSchema.class);
			List<String> sqls = genCreateSqlByLogicSchema(ls, dbSchema, dbtype);
			StringBuffer sb = new StringBuffer();
			for (String sql:sqls){
				sb.append(sql).append(";").append("\n");
			}
			FileUtils.writeStringToFile(new File(outputSql), sb.toString(), Charset.defaultCharset());
		}catch(Exception e){
			logger.error("", e);
		}
	}
	
	public static boolean genLogicSchemaFromDB(PropertiesConfiguration dbconf, String schemaName, String localLogicSchemaOutputFile){
		LogicSchema ls = new LogicSchema();
		Connection con = DBUtil.getConnection(dbconf);
		if (con==null){
			return false;
		}
		try {
			DatabaseMetaData dbmd = con.getMetaData();
			ResultSet tableResults = dbmd.getTables(null, schemaName, null, null);
			while(tableResults.next()) {
			    String tableName = tableResults.getString(3);
			    List<String> attrNames = new ArrayList<String>();
			    List<FieldType> attrTypes = new ArrayList<FieldType>();
			    ResultSet columnResults = dbmd.getColumns(null, schemaName, tableName, null);
			    while(columnResults.next()){
			        String columnName = columnResults.getString("COLUMN_NAME");
			        int columnType = columnResults.getInt("DATA_TYPE");
			        int columnSize = columnResults.getInt("COLUMN_SIZE");
			        int digits = columnResults.getInt("DECIMAL_DIGITS");
			        attrNames.add(columnName);
			        FieldType ft = getFieldType(columnType, columnSize, digits);
			        if (ft!=null){
			        	attrTypes.add(ft);
			        	logger.info(String.format("%s,%s,%d,%d,%d", tableName, columnName, columnType, columnSize, digits));
			        }else{
			        	logger.error(String.format("error: %s,%s,%d,%d,%d", tableName, columnName, columnType, columnSize, digits));
			        }
			        
			    }
			    columnResults.close();
			    ls.addAttributes(tableName, attrNames);
		        ls.addAttrTypes(tableName, attrTypes);
			}
			tableResults.close();
			Util.toLocalJsonFile(localLogicSchemaOutputFile, ls);
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (con!=null){
				try{
					con.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
		return true;
	}
}
