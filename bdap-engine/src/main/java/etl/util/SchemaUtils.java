package etl.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import etl.engine.LogicSchema;

public class SchemaUtils {
	public static final Logger logger = LogManager.getLogger(SchemaUtils.class);
	public static final String SCHEMA_FILENAME_EXTENSION = "schema";
	public static final String SCHEMA_INDEX_FILENAME = "schema-index." + SCHEMA_FILENAME_EXTENSION;
	
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
	
	private static class AttrNameCacheLoader extends CacheLoader<String, List<String>> {
		private FileSystem fs;
		private String path;
		private Class<? extends LogicSchema> clazz;
		public AttrNameCacheLoader(FileSystem fs, String path, Class<? extends LogicSchema> clazz) {
			this.fs = fs;
			this.path = path;
			this.clazz = clazz;
		}
		public List<String> load(String tableName) throws Exception {
			LogicSchema schema;
			if (fs != null)
				schema = (LogicSchema) HdfsUtil.fromDfsJsonFile(fs, path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			else
				schema = (LogicSchema) JsonUtil.fromLocalJsonFile(path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			if (schema != null) {
				List<String> attributes = schema.getAttrNames(tableName);
				if (attributes != null)
					return attributes;
				else
					throw new CacheItemNotFoundException("No attributes for table: " + tableName);
			} else
				throw new CacheItemNotFoundException("No such schema file for table: " + tableName);
		}
	}
	
	private static class AttrTypeCacheLoader extends CacheLoader<String, List<FieldType>> {
		private FileSystem fs;
		private String path;
		private Class<? extends LogicSchema> clazz;
		public AttrTypeCacheLoader(FileSystem fs, String path, Class<? extends LogicSchema> clazz) {
			this.fs = fs;
			this.path = path;
			this.clazz = clazz;
		}
		public List<FieldType> load(String tableName) throws Exception {
			LogicSchema schema;
			if (fs != null)
				schema = (LogicSchema) HdfsUtil.fromDfsJsonFile(fs, path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			else
				schema = (LogicSchema) JsonUtil.fromLocalJsonFile(path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			if (schema != null) {
				List<FieldType> attrTypes = schema.getAttrTypes(tableName);
				if (attrTypes != null)
					return attrTypes;
				else
					throw new CacheItemNotFoundException("No attributes for table: " + tableName);
			} else
				throw new CacheItemNotFoundException("No such schema file for table: " + tableName);
		}
	}
	
	public static LogicSchema fromLocalJsonPath(String path, Class<? extends LogicSchema> clazz) {
		File p = new File(path);
		if (p.isDirectory()) {
			if (!path.endsWith(File.separator))
				path = path + File.separator;
			LogicSchema index = (LogicSchema) JsonUtil.fromLocalJsonFile(path + SCHEMA_INDEX_FILENAME, clazz);
			AttrNameCacheLoader attrNameCacheLoader = new AttrNameCacheLoader(null, path, clazz);
			Cache<String, List<String>> attrNameCache = CacheBuilder.newBuilder().build(attrNameCacheLoader);
			index.setAttrNameMap(new CacheMap<List<String>>(attrNameCache));
			AttrTypeCacheLoader attrTypeCacheLoader = new AttrTypeCacheLoader(null, path, clazz);
			Cache<String, List<FieldType>> attrTypeCache = CacheBuilder.newBuilder().build(attrTypeCacheLoader);
			index.setAttrTypeMap(new CacheMap<List<FieldType>>(attrTypeCache));
			return index;
			
		} else {
			return clazz.cast(JsonUtil.fromLocalJsonFile(path, clazz));
		}
	}
	
	public static LogicSchema fromRemoteJsonPath(FileSystem fs, String path, Class<? extends LogicSchema> clazz) {
		Path p = new Path(path);
		try {
			if (fs.isDirectory(p)) {
				if (!path.endsWith(Path.SEPARATOR))
					path = path + Path.SEPARATOR;
				LogicSchema index = (LogicSchema) HdfsUtil.fromDfsJsonFile(fs, path + SCHEMA_INDEX_FILENAME, clazz);
				AttrNameCacheLoader attrNameCacheLoader = new AttrNameCacheLoader(fs, path, clazz);
				Cache<String, List<String>> attrNameCache = CacheBuilder.newBuilder().build(attrNameCacheLoader);
				index.setAttrNameMap(new CacheMap<List<String>>(attrNameCache));
				AttrTypeCacheLoader attrTypeCacheLoader = new AttrTypeCacheLoader(fs, path, clazz);
				Cache<String, List<FieldType>> attrTypeCache = CacheBuilder.newBuilder().build(attrTypeCacheLoader);
				index.setAttrTypeMap(new CacheMap<List<FieldType>>(attrTypeCache));
				return index;
				
			} else {
				return clazz.cast(HdfsUtil.fromDfsJsonFile(fs, path, clazz));
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}
	
	public static void toLocalJsonPath(String path, boolean directory, LogicSchema schema, Map<String, List<String>> attrIdMap) {
		if (directory) {
			LogicSchema tableSchema;
			File dir = new File(path);
			if (!dir.exists())
				dir.mkdirs();
			if (!path.endsWith(File.separator))
				path = path + File.separator;
			
			List<String> attrIds;
			
			for (Map.Entry<String, List<String>> entry: schema.getAttrNameMap().entrySet()) {
				tableSchema = new LogicSchema();
				tableSchema.addAttributes(entry.getKey(), entry.getValue());
				tableSchema.addAttrTypes(entry.getKey(), schema.getAttrTypes(entry.getKey()));
				
				attrIds = null;
				if (attrIdMap != null)
					/* In case the table-name -> attribute Id list map is provided */ 
					attrIds = attrIdMap.get(entry.getKey());
				
				if (attrIds != null)
					tableSchema.getAttrIdNameMap().putAll(filterAttrIdNameMapByAttrIds(schema.getAttrIdNameMap(), attrIds));
				else
					tableSchema.getAttrIdNameMap().putAll(filterAttrIdNameMap(schema.getAttrIdNameMap(), entry.getValue()));
				
				JsonUtil.toLocalJsonFile(path + entry.getKey() + "." + SCHEMA_FILENAME_EXTENSION, tableSchema);
			}
			
			schema.getAttrNameMap().clear();
			schema.getAttrTypeMap().clear();
			schema.getAttrIdNameMap().clear();
			schema.setIndex(true);
			JsonUtil.toLocalJsonFile(path + SCHEMA_INDEX_FILENAME, schema);
			
		} else {
			JsonUtil.toLocalJsonFile(path, schema);
		}
	}
	
	public static void toRemoteJsonPath(FileSystem fs, String path, boolean directory, LogicSchema schema, Map<String, List<String>> attrIdMap) {
		if (directory) {
			LogicSchema tableSchema;
			Path p = new Path(path);
			try {
				if (!fs.exists(p))
					fs.mkdirs(p);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			if (!path.endsWith(Path.SEPARATOR))
				path = path + Path.SEPARATOR;

			List<String> attrIds;
			
			for (Map.Entry<String, List<String>> entry: schema.getAttrNameMap().entrySet()) {
				tableSchema = new LogicSchema();
				tableSchema.addAttributes(entry.getKey(), entry.getValue());
				tableSchema.addAttrTypes(entry.getKey(), schema.getAttrTypes(entry.getKey()));
				
				attrIds = null;
				if (attrIdMap != null)
					/* In case the table-name -> attribute Id list map is provided */ 
					attrIds = attrIdMap.get(entry.getKey());
				
				if (attrIds != null)
					tableSchema.getAttrIdNameMap().putAll(filterAttrIdNameMapByAttrIds(schema.getAttrIdNameMap(), attrIds));
				else
					tableSchema.getAttrIdNameMap().putAll(filterAttrIdNameMap(schema.getAttrIdNameMap(), entry.getValue()));
				
				HdfsUtil.toDfsJsonFile(fs, path + entry.getKey() + "." + SCHEMA_FILENAME_EXTENSION, tableSchema);
			}
				
			schema.getAttrNameMap().clear();
			schema.getAttrTypeMap().clear();
			schema.getAttrIdNameMap().clear();
			schema.setIndex(true);
			HdfsUtil.toDfsJsonFile(fs, path + SCHEMA_INDEX_FILENAME, schema);
			
		} else {
			HdfsUtil.toDfsJsonFile(fs, path, schema);
		}
	}
	
	private static Map<String, String> filterAttrIdNameMap(Map<String, String> attrIdNameMap,
			List<String> attrNames) {
		Map<String, String> result = new HashMap<String, String>();
		for (Map.Entry<String, String> entry: attrIdNameMap.entrySet()) {
			for (String attrName: attrNames) {
				if (attrName.equals(entry.getValue())) {
					result.put(entry.getKey(), attrName);
					break;
				}
			}
		}
		return result;
	}
	
	private static Map<String, String> filterAttrIdNameMapByAttrIds(Map<String, String> attrIdNameMap,
			List<String> attrIds) {
		Map<String, String> result = new HashMap<String, String>();
		for (Map.Entry<String, String> entry: attrIdNameMap.entrySet()) {
			for (String attrId: attrIds) {
				if (attrId.equals(entry.getKey())) {
					result.put(attrId, entry.getValue());
					break;
				}
			}
		}
		return result;
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
			LogicSchema ls = fromLocalJsonPath(schemaFile, LogicSchema.class);
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
			JsonUtil.toLocalJsonFile(localLogicSchemaOutputFile, ls);
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
