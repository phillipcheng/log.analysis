package etl.cmd.dynschema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import scala.Tuple2;
import scala.Tuple3;
import bdap.util.HdfsUtil;
//
import bdap.util.Util;
import etl.cmd.SchemaETLCmd;
import etl.engine.LogicSchema;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.DBUtil;
import etl.util.FieldType;

public abstract class DynamicSchemaCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;
	private static final String CREATE_TABLES_SQL_KEY = "<<create-tables-sql>>";
	private static final Comparator<String> CREATE_TABLES_SQL_COMPARATOR = new Comparator<String>() {
		public int compare(String text1, String text2) { /* To sort ascendantly by timestamp */
			int i1 = text1.indexOf(":");
			int i2 = text2.indexOf(":");
			long t1;
			long t2;
			if (i1 != -1)
				t1 = Long.parseLong(text1.substring(0, i1));
			else
				t1 = 0;
			if (i2 != -1)
				t2 = Long.parseLong(text2.substring(0, i2));
			else
				t2 = 0;
			return (int)(t1 - t2);
		}
	};

	public static final @ConfigKey(type=DynSchemaProcessType.class,defaultValue="genCsv") String cfgkey_process_type="process.type";
	
	public static final Logger logger = LogManager.getLogger(DynamicSchemaCmd.class);

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

	public abstract DynamicTableSchema getDynamicTable(String input, LogicSchema ls) throws Exception;
	public abstract List<String[]> getValues() throws Exception;

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
					newAttrTypes.add(DBUtil.guessDBType(v));
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
	
	//tableName to csv
	@Override
	public List<Tuple2<String, String>> flatMapToPair(String key, String text, Mapper<LongWritable, Text, Text, Text>.Context context){
		super.init();
		try {
			List<Tuple2<String, String>> retList = new ArrayList<Tuple2<String, String>>();
			DynamicTableSchema dt = getDynamicTable(text, this.logicSchema);
			if (this.processType == DynSchemaProcessType.checkSchema || this.processType==DynSchemaProcessType.both){
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
			
			if (this.processType == DynSchemaProcessType.genCsv || this.processType==DynSchemaProcessType.both){
				//geneate csv
				String tableName = dt.getName();
				List<String> orgAttrs = logicSchema.getAttrNames(tableName);
				if (orgAttrs!=null){
					List<String> newAttrs = dt.getFieldNames();
					//gen new attr to old attr idx mapping
					Map<Integer,Integer> mapping = new HashMap<Integer, Integer>();//mapping.size == newAttrs.size
					for (int i=0; i<orgAttrs.size(); i++){
						String attr = orgAttrs.get(i);
						int idx = newAttrs.indexOf(attr);
						if (idx!=-1){
							mapping.put(idx, i);
						}
					}
					//gen csv
					int fieldNum = dt.getFieldNames().size();
					if (this.processType!=DynSchemaProcessType.genCsv){
						if (fieldNum>mapping.size()){
							logger.error(String.format("more value then type, schema not updated. table:%s, fields:%s, real field num:%d", 
									tableName, mapping, fieldNum));
							return null;
						}
					}
					List<String[]> vslist = getValues();
					for (int k=0; k<vslist.size(); k++){
						String[] fieldValues = vslist.get(k);
						String[] vs = new String[orgAttrs.size()];
						for (int i=0; i<fieldValues.length; i++){
							if (mapping.containsKey(i)){
								String v = fieldValues[i];
								int idx = mapping.get(i);
								vs[idx]=v;
							}
						}
						String csv = Util.getCsv(Arrays.asList(vs), false);
						if (context!=null){
							context.write(new Text(tableName), new Text(csv));
						}else{
							retList.add(new Tuple2<String, String>(tableName, csv));
						}
					}
				}
			}
			
			if (context==null){
				return retList;
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc,
			Class<? extends InputFormat> inputFormatClass) {
		JavaPairRDD<String, String> result = super.sparkProcessKeyValue(input, jsc, inputFormatClass);
		
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
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> values, 
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
				List<String> createSqls = Lists.newArrayList(values);
				Collections.sort(createSqls, CREATE_TABLES_SQL_COMPARATOR);
				logger.debug("Append {} sqls to file: {}", createSqls.size(), this.createTablesSqlFileName);
				
				for (String sql: createSqls) {
					sql = sql.substring(sql.lastIndexOf(":") + 1);
					HdfsUtil.appendDfsFile(currentFs, this.createTablesSqlFileName, Arrays.asList(sql));
				}

			} else {
				for (String v: values) {
					/* For spark */
					out.add(new Tuple3<String,String,String>(v, null, key));
				}
			}
			
		} else {
			for (String v: values)
				out.add(new Tuple3<String,String,String>(v, null, key));
		}
		return out;
	}
}
