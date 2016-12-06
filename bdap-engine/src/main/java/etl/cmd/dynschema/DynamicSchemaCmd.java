package etl.cmd.dynschema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import scala.Tuple2;
//
import bdap.util.Util;
import etl.cmd.SchemaETLCmd;
import etl.engine.LockType;
import etl.engine.LogicSchema;
import etl.engine.ProcessMode;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.zookeeper.lock.WriteLock;

public abstract class DynamicSchemaCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final String cfgkey_process_type="process.type";
	public static final String cfgkey_lock_type="lock.type";
	public static final String cfgkey_zookeeper_url="zookeeper.url";
	
	public static final Logger logger = LogManager.getLogger(DynamicSchemaCmd.class);
	
	public static final Object jvmLock = new Object(); //same JVM locker
	
	private DynSchemaProcessType processType = DynSchemaProcessType.genCsv;
	private LockType lockType = LockType.zookeeper;
	private String zookeeperUrl=null;
	private int batchSize=200;
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm, false);
		this.processType = DynSchemaProcessType.valueOf(super.getCfgString(cfgkey_process_type, DynSchemaProcessType.genCsv.toString()));
		this.lockType = LockType.valueOf(super.getCfgString(cfgkey_lock_type, LockType.zookeeper.toString()));
		this.zookeeperUrl = super.getCfgString(cfgkey_zookeeper_url, null);
		if (this.lockType==LockType.zookeeper && this.zookeeperUrl==null){
			logger.error(String.format("must specify %s for locktype:%s", cfgkey_zookeeper_url, lockType));
		}
		if (processType==DynSchemaProcessType.genCsv){
			this.lockType=LockType.none;
		}
		boolean loadSchema = true;
		if (pm == ProcessMode.Reduce){
			loadSchema = false;
		}
		if (loadSchema){
			if (lockType == LockType.zookeeper){
				super.loadSchema(this.zookeeperUrl);
			}else{
				super.loadSchema(null);
			}
		}
	}

	public abstract DynamicTableSchema getDynamicTable(String input, LogicSchema ls) throws Exception;
	public abstract List<String[]> getValues(int batchSize) throws Exception;
	public abstract int getValuesLength() throws Exception;

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
		ut.name = tableName;
		ut.id = dt.getId();
		List<String> orgSchemaAttributes = logicSchema.getAttrNames(tableName);
		//check new attribute
		List<String> curAttrNames = dt.getFieldNames();
		List<String> curAttrIds = dt.getFieldIds();
		String[] values = dt.getValueSample();
		List<String> newAttrNames = new ArrayList<String>();
		List<String> newAttrIds = new ArrayList<String>();
		List<FieldType> newAttrTypes = new ArrayList<FieldType>();
		for (int j=0; j<curAttrNames.size(); j++){
			String attrName = curAttrNames.get(j);
			String attrId = curAttrIds.get(j);
			if (orgSchemaAttributes==null || !orgSchemaAttributes.contains(attrName)){
				newAttrNames.add(attrName);
				newAttrIds.add(attrId);
				String v = null;
				if (values!=null){
					v = values[j];
				}
				if (dt.getTypes()==null){
					newAttrTypes.add(DBUtil.guessDBType(v));
				}else{
					newAttrTypes.add(dt.getTypes().get(j));
				}
			}
		}
		if (newAttrNames.size()>0){
			ut.attrNames=newAttrNames;
			ut.attrIds=newAttrIds;
			ut.attrTypes=newAttrTypes;
			return ut;
		}else{
			return null;
		}
	}
	
	private DynamicTableSchema updateLogicSchema(String text) throws Exception {
		super.fetchLogicSchema();//re-fetch
		DynamicTableSchema dt = getDynamicTable(text, this.logicSchema);
		UpdatedTable ut = checkSchemaUpdate(dt);
		if (ut!=null){//re-check
			Map<String, String> tableNameIdMap = new HashMap<String, String>();
			Map<String, List<String>> attrNamesMap = new HashMap<String, List<String>>();
			Map<String, List<String>> attrIdsMap = new HashMap<String, List<String>>();
			Map<String, List<FieldType>> attrTypesMap =new HashMap<String, List<FieldType>>();
			tableNameIdMap.put(ut.name, ut.id);
			attrNamesMap.put(ut.name, ut.attrNames);
			attrIdsMap.put(ut.name, ut.attrIds);
			attrTypesMap.put(ut.name, ut.attrTypes);
			super.updateSchema(tableNameIdMap, attrIdsMap, attrNamesMap, attrTypesMap);
		}
		return dt;
	}
	
	//tableName to csv
	public List<Tuple2<String, String>> flatMapToPair(String text, Mapper<LongWritable, Text, Text, Text>.Context context){
		super.init();
		try {
			DynamicTableSchema dt = getDynamicTable(text, this.logicSchema);
			if (this.processType == DynSchemaProcessType.checkSchema || this.processType==DynSchemaProcessType.both){
				UpdatedTable ut = checkSchemaUpdate(dt);
				if (ut!=null){//needs update
					logger.info(String.format("detect update needed, lock the schema for table %s", dt.getName()));
					//get the lock
					if (this.lockType==LockType.zookeeper){
						boolean finished=false;
						int maxWait = 10000;
						int curWait=0;
						int foreverWait=0;
						while (!finished){
							WriteLock lock = super.getZookeeperLock(this.zookeeperUrl);
							try {
								if (lock.lock()){
									while (!lock.isOwner() && curWait<maxWait){
										Thread.sleep(1000);
										curWait+=1000;
										logger.error(String.format("wait to get lock for %s, %d", dt.getName(), curWait));
									}
									if (curWait<maxWait){
										dt = updateLogicSchema(text);
										finished = true;
									}
								}else{
									Thread.sleep(1000);
									foreverWait++;
									if (foreverWait%10==0){
										logger.warn(String.format("get lock failed for %s for %d seconds", dt.getName(), foreverWait*10));
									}
								}
							}finally{
								super.releaseLock(lock);
								//logger.warn(String.format("release the lock for schema to change %s", dt.getName()));
							}
						}
					}else if (this.lockType==LockType.jvm){
						synchronized(jvmLock){
							dt = updateLogicSchema(text);
						}
					}else if (this.lockType==LockType.none){
						dt = updateLogicSchema(text);
					}
				}
			}
			
			if (this.processType == DynSchemaProcessType.genCsv || this.processType==DynSchemaProcessType.both){
				//geneate csv
				List<Tuple2<String, String>> retList = new ArrayList<Tuple2<String, String>>();
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
					String[] fieldValues = dt.getValueSample();
					if (this.processType!=DynSchemaProcessType.genCsv){
						if (fieldValues.length>mapping.size()){
							logger.error(String.format("more value then type, schema not updated. table:%s, fields:%s, values:%s", 
									tableName, mapping, Arrays.asList(fieldValues)));
							return null;
						}
					}
					int num = getValuesLength();
					logger.info(String.format("we have %d rows.", num));
					int bn = num%batchSize==0?num/batchSize:num/batchSize+1;
					for (int j=0; j<bn; j++){
						List<String[]> vslist = getValues(batchSize);
						for (int k=0; k<vslist.size(); k++){
							fieldValues = vslist.get(k);
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
					if (context==null){
						return retList;
					}
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	@Override
	public JavaPairRDD<String, String> sparkVtoKvProcess(JavaRDD<String> input, JavaSparkContext jsc){
		JavaPairRDD<String, String> ret = input.flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				return flatMapToPair(t, null).iterator();
			}
		});
		return ret;
	}
	
	/**
	 * @param row: each row is a xml file name
	 */
	@Override
	public Map<String, Object> mapProcess(long offset, String text, Mapper<LongWritable, Text, Text, Text>.Context context){
		logger.info(String.format("offset:%d, input text size:%s", offset, text.length()));
		try {
			List<Tuple2<String, String>> pairs = flatMapToPair(text, context);
			if (pairs!=null){
				for (Tuple2<String, String> pair: pairs){
					context.write(new Text(pair._1), new Text(pair._2));
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	/**
	 * @return newKey, newValue, baseOutputPath
	 */
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		for (Text v: values){
			mos.write(new Text(v.toString()), null, key.toString());
		}
		return null;
	}
}
