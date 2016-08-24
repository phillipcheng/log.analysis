package etl.tools;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import etl.engine.LogicSchema;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.Util;
import etl.util.VarType;

public class GenLogicSchema {
	public static final Logger logger = Logger.getLogger(GenLogicSchema.class);
	
	public static FieldType getFieldType(int type, int size, int digits){
		if (Types.TIMESTAMP == type){
			return new FieldType(VarType.TIMESTAMP);
		}else if (Types.VARCHAR == type){
			return new FieldType(VarType.STRING, size);
		}else if (Types.NUMERIC == type){
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
