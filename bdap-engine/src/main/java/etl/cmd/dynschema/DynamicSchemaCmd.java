package etl.cmd.dynschema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import scala.Tuple2;
//
import bdap.util.Util;
import etl.cmd.SchemaETLCmd;
import etl.util.DBUtil;
import etl.util.FieldType;

public abstract class DynamicSchemaCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final String cfgkey_process_type="process.type";
	
	public static final Logger logger = LogManager.getLogger(DynamicSchemaCmd.class);
	
	private DynSchemaProcessType processType = DynSchemaProcessType.genCsv;
	
	public DynamicSchemaCmd(){
		super();
	}
	
	public DynamicSchemaCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		this.processType = DynSchemaProcessType.valueOf(super.getCfgString(cfgkey_process_type, DynSchemaProcessType.genCsv.toString()));
	}
	
	//the added field names and types
	class UpdatedTable{
		String name;
		List<String> attrNames;
		List<FieldType> attrTypes;
	}
	
	private UpdatedTable checkSchemaUpdate(DynamicTable dt){
		String tableName = dt.getName();
		UpdatedTable ut = new UpdatedTable();
		ut.name = tableName;
		List<String> orgSchemaAttributes = logicSchema.getAttrNames(tableName);
		//check new attribute
		List<String> curAttrNames = dt.getFieldNames();
		String[] values = null;
		if (dt.getValues()!=null && dt.getValues().size()>0){
			values = dt.getValues().get(0);
		}
		List<String> newAttrNames = new ArrayList<String>();
		List<FieldType> newAttrTypes = new ArrayList<FieldType>();
		for (int j=0; j<curAttrNames.size(); j++){
			String mtc = curAttrNames.get(j);
			if (orgSchemaAttributes==null || !orgSchemaAttributes.contains(mtc)){
				newAttrNames.add(mtc);
				String v = null;
				if (values!=null){
					v = values[j];
				}
				newAttrTypes.add(DBUtil.guessDBType(v));
			}
		}
		if (newAttrNames.size()>0){
			ut.attrNames=newAttrNames;
			ut.attrTypes=newAttrTypes;
			return ut;
		}else{
			return null;
		}
	}
	
	private void updateSchema(DynamicTable dt){
		UpdatedTable ut = checkSchemaUpdate(dt);
		if (ut!=null){
			//lock
			//re-check
			List<String> createTableSqls = new ArrayList<String>();
			if (logicSchema.getAttrNames(ut.name)!=null){//update table
				//gen update sql
				createTableSqls.addAll(DBUtil.genUpdateTableSql(ut.attrNames, ut.attrTypes, ut.name, dbPrefix, super.getDbtype()));
				//update schema
				logicSchema.addAttributes(ut.name, ut.attrNames);
				logicSchema.addAttrTypes(ut.name, ut.attrTypes);
				
			}else{//new table
				//gen create sql
				createTableSqls.add(DBUtil.genCreateTableSql(ut.attrNames, ut.attrTypes, ut.name, dbPrefix, super.getDbtype()));
				//update to logic schema
				logicSchema.updateTableAttrs(ut.name, ut.attrNames);
				logicSchema.updateTableAttrTypes(ut.name, ut.attrTypes);
			}
			//gen copys.sql for reference
			super.updateDynSchema(createTableSqls);
			//finally unlock
		}
	}
	
	public abstract DynamicTable getDynamicTable(String input);
	
	//tableName to csv
	public List<Tuple2<String, String>> flatMapToPair(String text){
		super.init();
		try {
			DynamicTable dt = getDynamicTable(text);
			if (this.processType == DynSchemaProcessType.checkSchema || this.processType==DynSchemaProcessType.both){
				updateSchema(dt);
			}
			if (this.processType == DynSchemaProcessType.genCsv || this.processType==DynSchemaProcessType.both){
				//geneate csv
				List<Tuple2<String, String>> retList = new ArrayList<Tuple2<String, String>>();
				String tableName = dt.getName();
				List<String> orgAttrs = logicSchema.getAttrNames(tableName);
				List<String> newAttrs = dt.getFieldNames();
				//gen new attr to old attr idx mapping
				Map<Integer,Integer> mapping = new HashMap<Integer, Integer>();
				for (int i=0; i<orgAttrs.size(); i++){
					String attr = orgAttrs.get(i);
					int idx = newAttrs.indexOf(attr);
					if (idx!=-1){
						mapping.put(idx, i);
					}
				}
				//gen csv
				if (dt.getValues()!=null && dt.getValues().size()>0){
					String[] fieldValues = dt.getValues().get(0);
					if (fieldValues.length>mapping.size()){
						logger.error(String.format("more value then type, schema not updated. table:%s, fields:%s, values:%s", 
								tableName, mapping, Arrays.asList(fieldValues)));
						return null;
					}
					for (int k=0; k<dt.getValues().size(); k++){
						fieldValues = dt.getValues().get(k);
						String[] vs = new String[orgAttrs.size()];
						for (int i=0; i<fieldValues.length; i++){
							String v = fieldValues[i];
							int idx = mapping.get(i);
							vs[idx]=v;
						}
						String csv = Util.getCsv(Arrays.asList(vs), false);
						retList.add(new Tuple2<String, String>(tableName, csv));
					}
					return retList;
				}else{
					return null;
				}
			}
			return null;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	@Override
	public JavaPairRDD<String, String> sparkVtoKvProcess(JavaRDD<String> input, JavaSparkContext jsc){
		JavaPairRDD<String, String> ret = input.flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				return flatMapToPair(t).iterator();
			}
		});
		return ret;
	}
	
	/**
	 * @param row: each row is a xml file name
	 */
	@Override
	public Map<String, Object> mapProcess(long offset, String text, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			List<Tuple2<String, String>> pairs = flatMapToPair(text);
			for (Tuple2<String, String> pair: pairs){
				context.write(new Text(pair._1), new Text(pair._2));
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
		List<String[]> rets = new ArrayList<String[]>();
		for (Text v: values){
			//logger.info(String.format("v:%s", v.toString()));
			String[] ret = new String[]{v.toString(), null, key.toString()};
			rets.add(ret);
		}
		return rets;
	}
}
