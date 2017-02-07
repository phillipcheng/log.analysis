package etl.util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import bdap.util.HdfsUtil;
import bdap.util.JsonUtil;
import etl.engine.ETLCmd;
import etl.engine.LogicSchema;
import etl.engine.types.DBType;

public class SchemaUtils {
	public static final Logger logger = LogManager.getLogger(SchemaUtils.class);
	public static final String SCHEMA_FILENAME_EXTENSION = "schema";
	public static final String SCHEMA_INDEX_FILENAME = "schema-index." + SCHEMA_FILENAME_EXTENSION;
	
	public static FieldType getFieldType(int type, int size, int digits){
		if (java.sql.Types.TIMESTAMP == type){
			return new FieldType(VarType.TIMESTAMP);
		}else if (java.sql.Types.VARCHAR == type){
			return new FieldType(VarType.STRING, size);
		}else if (java.sql.Types.NUMERIC == type){
			return new FieldType(VarType.NUMERIC, size, digits);
		}else if (java.sql.Types.DECIMAL == type){
			return new FieldType(VarType.NUMERIC, size, digits);
		}else if (java.sql.Types.BIGINT == type){
			return new FieldType(VarType.INT);
		}else if (java.sql.Types.DATE == type){
			return new FieldType(VarType.DATE);
		}else{
			logger.error(String.format("not supported:%d", type));
			return null;
		}
	}
	
	private static class AttrNameCacheLoader extends CacheLoader<String, List<String>> implements Serializable {
		private static final long serialVersionUID = 1L;
		private transient FileSystem fs;
		private String defaultFs;
		private String path;
		private Class<? extends LogicSchema> clazz;
		private Map<String, String> attrIdNameMap;
		public AttrNameCacheLoader(String defaultFs, String path, Class<? extends LogicSchema> clazz, Map<String, String> attrIdNameMap) {
			this.defaultFs = defaultFs;
			this.fs = HdfsUtil.getHadoopFs(defaultFs);
			this.path = path;
			this.clazz = clazz;
			this.attrIdNameMap = attrIdNameMap;
		}
		public List<String> load(String tableName) throws Exception {
			LogicSchema schema;
			
			if (fs == null) // Try init in case of remote job
				fs = HdfsUtil.getHadoopFs(defaultFs);
			
			if (fs != null)
				schema = (LogicSchema) HdfsUtil.fromDfsJsonFile(fs, path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			else
				schema = (LogicSchema) JsonUtil.fromLocalJsonFile(path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			if (schema != null) {
				List<String> attributes = schema.getAttrNames(tableName);
				
				/* Load the attribute Id name map into the index schema */
				attrIdNameMap.putAll(schema.getAttrIdNameMap());
				
				if (attributes != null)
					return attributes;
				else
					throw new CacheItemNotFoundException("No attributes for table: " + tableName);
			} else
				throw new CacheItemNotFoundException("No such schema file for table: " + tableName);
		}
	}
	
	private static class AttrTypeCacheLoader extends CacheLoader<String, List<FieldType>> implements Serializable {
		private static final long serialVersionUID = 1L;
		private transient FileSystem fs;
		private String defaultFs;
		private String path;
		private Class<? extends LogicSchema> clazz;
		private Map<String, String> attrIdNameMap;
		public AttrTypeCacheLoader(String defaultFs, String path, Class<? extends LogicSchema> clazz, Map<String, String> attrIdNameMap) {
			this.defaultFs = defaultFs;
			this.fs = HdfsUtil.getHadoopFs(defaultFs);
			this.path = path;
			this.clazz = clazz;
			this.attrIdNameMap = attrIdNameMap;
		}
		public List<FieldType> load(String tableName) throws Exception {
			LogicSchema schema;
			
			if (fs == null) // Try init in case of remote job
				fs = HdfsUtil.getHadoopFs(defaultFs);
			
			if (fs != null)
				schema = (LogicSchema) HdfsUtil.fromDfsJsonFile(fs, path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			else
				schema = (LogicSchema) JsonUtil.fromLocalJsonFile(path + tableName + "." + SCHEMA_FILENAME_EXTENSION, clazz);
			if (schema != null) {
				List<FieldType> attrTypes = schema.getAttrTypes(tableName);
				
				/* Load the attribute Id name map into the index schema */
				attrIdNameMap.putAll(schema.getAttrIdNameMap());
				
				if (attrTypes != null)
					return attrTypes;
				else
					throw new CacheItemNotFoundException("No attribute types for table: " + tableName);
			} else
				throw new CacheItemNotFoundException("No such schema file for table: " + tableName);
		}
	}
	
	private static class AttrNameRemovalListener implements RemovalListener<String, List<String>>, Serializable {
		private static final long serialVersionUID = 1L;

		public void onRemoval(RemovalNotification<String, List<String>> notification) {
			logger.debug("Attribute removed: {}", notification);
		}
	}
	
	private static class AttrTypeRemovalListener implements RemovalListener<String, List<FieldType>>, Serializable {
		private static final long serialVersionUID = 1L;

		public void onRemoval(RemovalNotification<String, List<FieldType>> notification) {
			logger.debug("Attribute type removed: {}", notification);
		}
	}
	
	private final static AttrNameRemovalListener ATTR_NAME_REMOVAL_LISTENER = new AttrNameRemovalListener();
	private final static AttrTypeRemovalListener ATTR_TYPE_REMOVAL_LISTENER = new AttrTypeRemovalListener();
	
	public static boolean existsLocalJsonPath(String path) {
		File p = new File(path);
		try {
			if (p.exists()) {
				if (p.isDirectory()) {
					if (!path.endsWith(File.separator))
						path = path + File.separator;
					p = new File(path + SCHEMA_INDEX_FILENAME);
					return p.exists();
				} else {
					return true;
				}
			} else {
				return false;
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return false;
		}
	}
	
	public static boolean existsRemoteJsonPath(String defaultFs, String path) {
		Path p = new Path(path);
		FileSystem fs = HdfsUtil.getHadoopFs(defaultFs);
		try {
			if (fs.exists(p)) {
				if (fs.isDirectory(p)) {
					if (!path.endsWith(Path.SEPARATOR))
						path = path + Path.SEPARATOR;
					p = new Path(path + SCHEMA_INDEX_FILENAME);
					return fs.exists(p);
				} else {
					return true;
				}
			} else {
				return false;
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return false;
		}
	}

	public static LogicSchema newLocalInstance(String path) {
		LogicSchema schema = new LogicSchema();
		File p = new File(path);
		schema.setIndex(p.exists() && p.isDirectory());
		return schema;
	}

	public static LogicSchema newRemoteInstance(String defaultFs, String path) {
		LogicSchema schema = new LogicSchema();
		Path p = new Path(path);
		FileSystem fs = HdfsUtil.getHadoopFs(defaultFs);
		try {
			schema.setIndex(path.endsWith(Path.SEPARATOR) || (fs.exists(p) && fs.isDirectory(p)));
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		return schema;
	}
	
	public static LogicSchema fromLocalJsonPath(String path, Class<? extends LogicSchema> clazz) {
		File p = new File(path);
		if (p.isDirectory()) {
			if (!path.endsWith(File.separator))
				path = path + File.separator;
			LogicSchema index = (LogicSchema) JsonUtil.fromLocalJsonFile(path + SCHEMA_INDEX_FILENAME, clazz);
			AttrNameCacheLoader attrNameCacheLoader = new AttrNameCacheLoader(null, path, clazz, index.getAttrIdNameMap());
			Cache<String, List<String>> attrNameCache = CacheBuilder.newBuilder().maximumSize(Long.MAX_VALUE).removalListener(ATTR_NAME_REMOVAL_LISTENER).build(attrNameCacheLoader);
			index.setAttrNameMap(new CacheMap<List<String>>(attrNameCache));
			AttrTypeCacheLoader attrTypeCacheLoader = new AttrTypeCacheLoader(null, path, clazz, index.getAttrIdNameMap());
			Cache<String, List<FieldType>> attrTypeCache = CacheBuilder.newBuilder().maximumSize(Long.MAX_VALUE).removalListener(ATTR_TYPE_REMOVAL_LISTENER).build(attrTypeCacheLoader);
			index.setAttrTypeMap(new CacheMap<List<FieldType>>(attrTypeCache));
			return index;
		} else {
			return clazz.cast(JsonUtil.fromLocalJsonFile(path, clazz));
		}
	}
	
	public static LogicSchema fromRemoteJsonPath(String defaultFs, String path, Class<? extends LogicSchema> clazz) {
		Path p = new Path(path);
		try {
			FileSystem fs = HdfsUtil.getHadoopFs(defaultFs);
			if (fs.isDirectory(p)) {
				if (!path.endsWith(Path.SEPARATOR))
					path = path + Path.SEPARATOR;
				LogicSchema index = (LogicSchema) HdfsUtil.fromDfsJsonFile(fs, path + SCHEMA_INDEX_FILENAME, clazz);
				AttrNameCacheLoader attrNameCacheLoader = new AttrNameCacheLoader(defaultFs, path, clazz, index.getAttrIdNameMap());
				Cache<String, List<String>> attrNameCache = CacheBuilder.newBuilder().maximumSize(Long.MAX_VALUE).removalListener(ATTR_NAME_REMOVAL_LISTENER).build(attrNameCacheLoader);
				index.setAttrNameMap(new CacheMap<List<String>>(attrNameCache));
				AttrTypeCacheLoader attrTypeCacheLoader = new AttrTypeCacheLoader(defaultFs, path, clazz, index.getAttrIdNameMap());
				Cache<String, List<FieldType>> attrTypeCache = CacheBuilder.newBuilder().maximumSize(Long.MAX_VALUE).removalListener(ATTR_TYPE_REMOVAL_LISTENER).build(attrTypeCacheLoader);
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
	
	public static void toRemoteJsonPath(String defaultFs, String path, boolean directory, LogicSchema schema, Map<String, List<String>> attrIdMap) {
		FileSystem fs = HdfsUtil.getHadoopFs(defaultFs);
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
			
			if (!(schema.getAttrNameMap() instanceof CacheMap)) {
				AttrNameCacheLoader attrNameCacheLoader = new AttrNameCacheLoader(defaultFs, path, schema.getClass(), schema.getAttrIdNameMap());
				Cache<String, List<String>> attrNameCache = CacheBuilder.newBuilder().maximumSize(Long.MAX_VALUE).removalListener(ATTR_NAME_REMOVAL_LISTENER).build(attrNameCacheLoader);
				schema.setAttrNameMap(new CacheMap<List<String>>(attrNameCache));
			}
			
			if (!(schema.getAttrTypeMap() instanceof CacheMap)) {
				AttrTypeCacheLoader attrTypeCacheLoader = new AttrTypeCacheLoader(defaultFs, path, schema.getClass(), schema.getAttrIdNameMap());
				Cache<String, List<FieldType>> attrTypeCache = CacheBuilder.newBuilder().maximumSize(Long.MAX_VALUE).removalListener(ATTR_TYPE_REMOVAL_LISTENER).build(attrTypeCacheLoader);
				schema.setAttrTypeMap(new CacheMap<List<FieldType>>(attrTypeCache));
			}
			
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
		return genCreateSqlByLogicSchema(ls, dbSchema, dbtype, StoreFormat.text);
	}
	
	public static List<String> genCreateSqlByLogicSchema(LogicSchema ls, String dbSchema, DBType dbtype, StoreFormat sf){
		List<String> sqls = new ArrayList<String>();
		for (String tn: ls.getTableNames()){
			List<String> attrNames = ls.getAttrNames(tn);
			List<FieldType> attrTypes = ls.getAttrTypes(tn);
			String sql = DBUtil.genCreateTableSql(attrNames, attrTypes, tn, dbSchema, dbtype, sf);
			sqls.add(sql);
		}
		return sqls;
	}
	
	public static void genCreateSqls(String schemaFile, String outputSql, String dbSchema, DBType dbtype) throws Exception {
		genCreateSqls(schemaFile, outputSql, dbSchema, dbtype, StoreFormat.text);
	}
	public static void genCreateSqls(String schemaFile, String outputSql, String dbSchema, DBType dbtype, StoreFormat sf) throws Exception {
		LogicSchema ls = fromLocalJsonPath(schemaFile, LogicSchema.class);
		List<String> sqls = genCreateSqlByLogicSchema(ls, dbSchema, dbtype, sf);
		StringBuffer sb = new StringBuffer();
		for (String sql:sqls){
			sb.append(sql).append(";").append("\n");
		}
		FileUtils.writeStringToFile(new File(outputSql), sb.toString(), Charset.defaultCharset());
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
	
//convert to parquet schema
	private static final int MAX_DECIMAL_BYTES = 16;
	public static long maxPrecision(int numBytes) {
		return Math.round( // convert double to long
				Math.floor(Math.log10( // number of base-10 digits
						Math.pow(2, 8 * numBytes - 1) - 1) // max value stored
														   // in numBytes
				));
	}
	
	public static int minBytes(int precision) {
		int bytes;
		for (bytes = 1; bytes < MAX_DECIMAL_BYTES; bytes ++)
			if (precision < maxPrecision(bytes))
				return bytes;
		return bytes;
	}
	
	public static MessageType convertToParquetSchema(LogicSchema logicSchema, String tableName) {
		MessageTypeBuilder m = Types.buildMessage();
		if (logicSchema.hasTable(tableName)) {
			List<String> attrs = logicSchema.getAttrNames(tableName);
			List<FieldType> attrTypes = logicSchema.getAttrTypes(tableName);
			int i;
			if (attrs != null && attrTypes != null && attrs.size() > 0 && attrs.size() == attrTypes.size()) {
				FieldType type;
				String name;
				for (i = 0; i < attrs.size(); i ++) {
					type = attrTypes.get(i);
					name = attrs.get(i);
					switch (type.getType()) {
					case STRING:
					case JAVASCRIPT:
					case REGEXP:
					case GLOBEXP:
					case STRINGLIST:
						m.addField(Types.required(PrimitiveTypeName.BINARY) /* Hive doesn't support .length(type.getSize()) */ .as(OriginalType.UTF8).named(name));
						break;
					case TIMESTAMP:
						m.addField(Types.required(PrimitiveTypeName.INT64).as(OriginalType.TIMESTAMP_MILLIS).named(name));
						break;
					case DATE:
						m.addField(Types.required(PrimitiveTypeName.INT64).as(OriginalType.DATE).named(name));
						break;
					case NUMERIC:
						m.addField(Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(minBytes(type.getPrecision()))
								.as(OriginalType.DECIMAL).precision(type.getPrecision()).scale(type.getScale()).named(name));
						break;
					case INT:
						m.addField(Types.required(PrimitiveTypeName.INT32).as(OriginalType.INT_32).named(name));
						break;
					case FLOAT:
						m.addField(Types.required(PrimitiveTypeName.FLOAT).named(name));
						break;
					case BOOLEAN:
						m.addField(Types.required(PrimitiveTypeName.BOOLEAN).named(name));
						break;
					case ARRAY:
					case LIST:
					case OBJECT:
						m.addField(Types.required(PrimitiveTypeName.BINARY).named(name));
						break;
					default:
						logger.error("Unknown type: {}, set as binary", type);
						m.addField(Types.required(PrimitiveTypeName.BINARY).named(name));
						break;
					}
				}
			} else {
				/* Empty schema only has id */
				m.addField(Types.required(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named("id"));
			}
		} else {
			/* Empty schema only has id */
			m.addField(Types.required(PrimitiveTypeName.INT64).as(OriginalType.INT_64).named("id"));
		}
		return m.named(tableName);
	}
//convert to spark sql struct type
	public static StructType convertToSparkSqlSchema(LogicSchema logicSchema, String tableName) {
		if (logicSchema.hasTable(tableName)){
			StructType st = new StructType();
			List<String> attrs = logicSchema.getAttrNames(tableName);
			List<FieldType> attrTypes = logicSchema.getAttrTypes(tableName);
			FieldType type;
			String name;
			for (int i = 0; i < attrs.size(); i ++) {
				name = attrs.get(i);
				type = attrTypes.get(i);
				DataType dt = null;
				switch (type.getType()) {
					case STRING:
						dt = DataTypes.StringType;
						break;
					case TIMESTAMP:
						dt = DataTypes.TimestampType;
						break;
					case DATE:
						dt = DataTypes.DateType;
						break;
					case NUMERIC:
						dt = DataTypes.createDecimalType(type.getPrecision(), type.getScale());
						break;
					case INT:
						dt = DataTypes.IntegerType;
						break;
					case FLOAT:
						dt = DataTypes.FloatType;
						break;
					case BOOLEAN:
						dt = DataTypes.BooleanType;
						break;
					default:
						logger.error("Unknown type: {}, set as binary", type);
						dt = DataTypes.BinaryType;
						break;
				}
				st = st.add(DataTypes.createStructField(name, dt, true));
			}
			return st;
		}else{
			logger.error(String.format("table %s not found in logicSchema:%s.", tableName, logicSchema.getTableNames()));
			return null;
		}
	}
	
	public static Object[] convertFromStringValues(LogicSchema logicSchema, String tableName, 
			String row, String csvValueSep, boolean inputEndwithDelimiter, boolean skipHeader) throws Exception {
		CSVParser csvParser=CSVParser.parse(row, 
				CSVFormat.DEFAULT.withTrim().withDelimiter(csvValueSep.charAt(0)).
				withTrailingDelimiter(inputEndwithDelimiter).withSkipHeaderRecord(skipHeader));
	    CSVRecord csvr = csvParser.getRecords().get(0);
	    List<FieldType> ftl = null;
		if (ETLCmd.SINGLE_TABLE.equals(tableName)){
			ftl = Collections.nCopies(csvr.size(), new FieldType(VarType.STRING));
		}else{
			ftl = logicSchema.getAttrTypes(tableName);
		}
		
	    if (ETLCmd.SINGLE_TABLE.equals(tableName) || logicSchema.hasTable(tableName)){
			if (ftl.size()!=csvr.size()){
				logger.error(String.format("input %s not match with schema %s", row, ftl));
				return null;
			}
			Object[] ret = new Object[csvr.size()];
			for (int i=0; i<ftl.size(); i++){
				FieldType ft = ftl.get(i);
				String fv = csvr.get(i).trim();
				if ("".equals(fv)){
					ret[i]=null;
				}else{
					if (VarType.TIMESTAMP==ft.getType()){
						Timestamp ts = null;
						String sdtformat = ft.getDtformat();
						if (sdtformat!=null){
							Date dt = GroupFun.getStandardizeDt(fv, sdtformat);
							if (dt!=null){
								ts = new Timestamp(dt.getTime());
							}else{
								logger.error(String.format("can't standardize %s with format:%s", fv, sdtformat));
							}
						}else{
							try {
								ts = Timestamp.valueOf(fv);
							}catch(Exception e){
								logger.error(String.format("can't parse timestamp field %s", fv));
							}
						}
						ret[i]=ts;
					}else if (VarType.NUMERIC==ft.getType()){
						java.math.BigDecimal d = null;
						try {
							d=  java.math.BigDecimal.valueOf(Double.valueOf(fv));
						}catch(Exception e){
							logger.warn(String.format("can't parse %s as double", fv));
						}
						ret[i]=d;
					}else if (VarType.INT==ft.getType()){
						Integer d = null;
						try {
							d= Integer.valueOf(fv);
						}catch(Exception e){
							logger.warn(String.format("can't parse %s as integer", fv));
						}
						ret[i]=d;
					}else{
						ret[i]=fv;
					}
				}
			}
			return ret;
		}else{
			logger.error(String.format("table not found: %s in logicSchema: %s.", tableName, logicSchema.getTableNames()));
			return null;
		}
	}
	
	public static boolean isNumericType(StructField sf){
		if (sf.dataType()==DataTypes.DoubleType||sf.dataType()==DataTypes.FloatType
				||sf.dataType()==DataTypes.IntegerType||sf.dataType()==DataTypes.LongType){
			return true;
		}else{
			return false;
		}
	}
	public static String convertToString(Row row, String csvValueSep) {
		StringBuffer sb = new StringBuffer();
		StructType st = row.schema();
		for (int i=0; i<row.length(); i++){
			StructField sf = st.fields()[i];
			if (row.isNullAt(i)){
				if (isNumericType(sf)){//set numeric null to 0
					sb.append("0");
				}else{
					sb.append("");
				}
			}else{
				sb.append(row.get(i).toString());
			}
			if (i<row.length()-1){
				sb.append(csvValueSep);
			}
		}
		return sb.toString();
	}
}
