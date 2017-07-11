package etl.cmd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import bdap.util.HdfsUtil;
//
import bdap.util.Util;
import etl.cmd.dynschema.DynSchemaProcessType;
import etl.cmd.dynschema.DynamicTableSchema;
import etl.engine.LogicSchema;
import etl.engine.types.InputFormatType;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;
import etl.util.DBUtil;
import etl.util.FieldType;
import scala.Tuple2;
import scala.Tuple3;

public abstract class DynamicXmlSchemaCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;
	private static final String CREATE_TABLES_SQL_KEY = "<<create-tables-sql>>";

	public static final @ConfigKey(type=DynSchemaProcessType.class,defaultValue="genCsv") String cfgkey_process_type="process.type";
	
	public static final Logger logger = LogManager.getLogger(DynamicXmlSchemaCmd.class);

	private DynSchemaProcessType processType = DynSchemaProcessType.genCsv;
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		if (pm == ProcessMode.Reduce)
			super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm, false);
		else
			super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm, true);
		this.processType = DynSchemaProcessType.valueOf(super.getCfgString(cfgkey_process_type, DynSchemaProcessType.genCsv.toString()));
		/* if (processType==DynSchemaProcessType.genCsv){
			this.lockType=LockType.none;
		} */
	}

	public abstract List<DynamicTableSchema> getDynamicTable(String input, LogicSchema ls) throws Exception;


	//the added field names and types
	class UpdatedTable{
		String id;
		String name;
		List<String> attrNames;
		List<String> attrIds;
		List<FieldType> attrTypes;
	}
	
	private UpdatedTable checkSchemaUpdate(DynamicTableSchema dt){
		String tableName = dt.getName();
		UpdatedTable ut = new UpdatedTable();
		List<String> orgSchemaAttributes;
		ut.name = tableName;
		ut.id = dt.getId();
		if (logicSchema.hasTable(tableName))
			orgSchemaAttributes = logicSchema.getAttrNames(tableName);
		else
			orgSchemaAttributes = null;
		//check new attribute
		List<String> curAttrNames = dt.getFieldNames();
		List<String> curAttrIds = dt.getFieldIds();
		List<String> newAttrNames = new ArrayList<String>();
		List<String> newAttrIds = new ArrayList<String>();
		List<FieldType> newAttrTypes = new ArrayList<FieldType>();
		for (int j=0; j<curAttrNames.size(); j++){//for every attr
			String attrName = curAttrNames.get(j);
			if (orgSchemaAttributes==null || !orgSchemaAttributes.contains(attrName)){
				newAttrNames.add(attrName);
				if (curAttrIds!=null){
					String attrId = curAttrIds.get(j);
					newAttrIds.add(attrId);
				}
				FieldType ft = null;
				if (dt.getTypes()!=null && j<dt.getTypes().size()){
					ft = dt.getTypes().get(j);
				}
				if (ft==null){
					String v = null;
					if (dt.getValueSample()!=null){
						v = dt.getValueSample()[j];
					}
					newAttrTypes.add(DBUtil.guessDBType(attrName,v));
				}else{
					newAttrTypes.add(ft);
				}
			}
		}
		if (newAttrNames.size()>0){
			ut.attrNames=newAttrNames;
			if (curAttrIds!=null){
				ut.attrIds=newAttrIds;
			}
			ut.attrTypes=newAttrTypes;
			return ut;
		}else{
			return null;
		}
	}
	
	@Override
	public boolean hasReduce(){
		return true;
	}
	
	//tableName to csv
	@Override
	public List<Tuple2<String, String>> flatMapToPair(String key, String text, Mapper<LongWritable, Text, Text, Text>.Context context){
		super.init();
		try {
			List<Tuple2<String, String>> retList = new ArrayList<Tuple2<String, String>>();
			if (this.processType == DynSchemaProcessType.checkSchema || this.processType==DynSchemaProcessType.both){
				List<DynamicTableSchema> dtList = getDynamicTable(text, this.logicSchema);
				for(DynamicTableSchema dt:dtList){
					UpdatedTable ut = checkSchemaUpdate(dt);
					if (ut != null) {//needs update
						logger.info(String.format("detect update needed, lock the schema for table %s", dt.getName()));
						List<String> logInfo = updateSchema(ut.id, ut.name, ut.attrIds, ut.attrNames, ut.attrTypes);
						
						if (logInfo != null) {
							for (String info: logInfo) {
								if (context!=null){
									context.write(new Text(CREATE_TABLES_SQL_KEY), new Text(info));
								}else{
									retList.add(new Tuple2<String, String>(CREATE_TABLES_SQL_KEY, info));
								}
							}
						}
					}
				}
				
				if (context==null){
					return retList;
				}
			}
			
			if (this.processType == DynSchemaProcessType.genCsv || this.processType==DynSchemaProcessType.both){
				List<Tuple2<String, String>> csvList = parseXml2Values(key, text, context);
				if (context!=null){
					for(Tuple2<String, String> item: csvList){
						context.write(new Text(item._1), new Text(item._2));
					}
				}else{
					retList.addAll(csvList);
				}
			}
			
			
			
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	
	abstract protected List<Tuple2<String, String>>  parseXml2Values(String key, String text, Mapper<LongWritable, Text, Text, Text>.Context context);
	
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc,
			InputFormatType ift, SparkSession spark) {
		JavaPairRDD<String, String> result = super.sparkProcessKeyValue(input, jsc, ift, spark);
		
		if (processType == DynSchemaProcessType.checkSchema || processType == DynSchemaProcessType.both) {
			input = result.filter(new Function<Tuple2<String, String>, Boolean>(){
				private static final long serialVersionUID = 1L;
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					return v1 != null && v1._1 != null && CREATE_TABLES_SQL_KEY.equals(v1._1);
				}
			});
			JavaRDD<String> createTablesSql = input.values();
			createTablesSql = createTablesSql.sortBy(new Function<String, Long>() {
				private static final long serialVersionUID = 1L;
				public Long call(String v1) throws Exception {
					int i1 = v1.indexOf(":");
					if (i1 != -1)
						return Long.parseLong(v1.substring(0, i1));
					else
						return Long.valueOf(0);
				}
			}, true, 1);
			createTablesSql = createTablesSql.map(new Function<String, String>() {
				private static final long serialVersionUID = 1L;
				public String call(String v1) throws Exception {
					return v1.substring(v1.lastIndexOf(":") + 1);
				}
			});
			createTablesSql.saveAsTextFile(this.createTablesSqlFileName);
			
			return result.filter(new Function<Tuple2<String, String>, Boolean>(){
				private static final long serialVersionUID = 1L;
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					return v1 != null && v1._1 != null && !CREATE_TABLES_SQL_KEY.equals(v1._1);
				}
			});
			
		} else {
			return result;
		}
	}

	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<? extends Object> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<Tuple3<String,String,String>> out = new ArrayList<Tuple3<String,String,String>>();
		if (CREATE_TABLES_SQL_KEY.equals(key)) {
			if (mos != null) {
				/* For Hadoop MR */
				FileSystem currentFs;
				if (fs == null) {
					try {
						String fs_key = "fs.defaultFS";
						Configuration conf = new Configuration();
						if (defaultFs!=null){
							conf.set(fs_key, defaultFs);
						}
						currentFs = FileSystem.get(conf);
					}catch(Exception e){
						logger.error("", e);
						currentFs = null;
					}
				} else {
					currentFs = fs;
				}
				List<String> createSqls = new ArrayList<String>();
				for (Object v: values){
					createSqls.add(v.toString());
				}
				
				Collections.sort(createSqls, CREATE_TABLES_SQL_COMPARATOR);
				logger.debug("Append {} sqls to file: {}", createSqls.size(), this.createTablesSqlFileName);
				
				for (String sql: createSqls) {
					sql = sql.substring(sql.lastIndexOf(":") + 1);
					HdfsUtil.appendDfsFile(currentFs, this.createTablesSqlFileName, Arrays.asList(sql));
				}
			} else {
				for (Object v: values) {
					/* For spark */
					out.add(new Tuple3<String,String,String>(v.toString(), null, key));
				}
			}
			
		} else {
			for (Object v: values)
				out.add(new Tuple3<String,String,String>(v.toString(), null, key));
		}
		return out;
	}
}
