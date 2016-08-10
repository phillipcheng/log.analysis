package etl.tools;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.LogicSchema;
import etl.util.DBUtil;
import etl.util.Util;

public class GenLogicSchema {
	public static final Logger logger = Logger.getLogger(GenLogicSchema.class);
	
	public static String getTypeString(int type, int size, int digits){
		if (Types.TIMESTAMP == type){
			return "timestamp";
		}else if (Types.VARCHAR == type){
			return String.format("varchar(%d)", size);
		}else if (Types.NUMERIC == type){
			return String.format("numeric(%d,%d)", size, digits);
		}else{
			logger.error(String.format("unsupported type:%d", type));
			return "";
		}
	}
	
	public static void genLogicSchemaFromDB(PropertiesConfiguration dbconf, String schemaName, String localLogicSchemaOutputFile){
		LogicSchema ls = new LogicSchema();
		Connection con = DBUtil.getConnection(dbconf);
		try {
			DatabaseMetaData dbmd = con.getMetaData();
			ResultSet tableResults = dbmd.getTables(null, schemaName, null, null);
			while(tableResults.next()) {
			    String tableName = tableResults.getString(3);
			    List<String> attrNames = new ArrayList<String>();
			    List<String> attrTypes = new ArrayList<String>();
			    ResultSet columnResults = dbmd.getColumns(null, schemaName, tableName, null);
			    while(columnResults.next()){
			        String columnName = columnResults.getString("COLUMN_NAME");
			        int columnType = columnResults.getInt("DATA_TYPE");
			        int columnSize = columnResults.getInt("COLUMN_SIZE");
			        int digits = columnResults.getInt("DECIMAL_DIGITS");
			        attrNames.add(columnName);
			        attrTypes.add(getTypeString(columnType, columnSize, digits));
			        logger.info(String.format("%s,%s,%d,%d,%d", tableName, columnName, columnType, columnSize, digits));
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
	}

}
