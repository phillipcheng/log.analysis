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
import scala.Tuple2;
import scala.Tuple3;
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
		ut.name = tableName;
		ut.id = dt.getId();
		List<String> orgSchemaAttributes = logicSchema.getAttrNames(tableName);
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
			DynamicTableSchema dt = getDynamicTable(text, this.logicSchema);
			if (this.processType == DynSchemaProcessType.checkSchema || this.processType==DynSchemaProcessType.both){
				UpdatedTable ut = checkSchemaUpdate(dt);
				if (ut != null) {//needs update
					logger.info(String.format("detect update needed, lock the schema for table %s", dt.getName()));
					
					updateSchema(ut.id, ut.name, ut.attrIds, ut.attrNames, ut.attrTypes);
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
	
	/**
	 * @param row: each row is a xml file name
	 */
	@Override
	public Map<String, Object> mapProcess(long offset, String text, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			List<Tuple2<String, String>> pairs = flatMapToPair(null, text, context);
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
	
	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<Tuple3<String,String,String>> out = new ArrayList<Tuple3<String,String,String>>();
		for (String v: values){
			if(mos!=null){
				mos.write(new Text(v), null, key);
			}else{
				out.add(new Tuple3<String,String,String>(v, null, key));
			}
		}
		return out;
	}
	
	/**
	 * @return newKey, newValue, baseOutputPath
	 */
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<String> svalues = new ArrayList<String>();
		Iterator<Text> vit = values.iterator();
		while (vit.hasNext()){
			svalues.add(vit.next().toString());
		}
		List<String[]> ret = new ArrayList<String[]>();	
		List<Tuple3<String, String, String>> output = reduceByKey(key.toString(), svalues, context, mos);
		for (Tuple3<String, String, String> t: output){
			ret.add(new String[]{t._1(), t._2(), t._3()});
		}
		return ret;
	}
}
