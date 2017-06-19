package etl.cmd.dynschema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

import bdap.util.HdfsUtil;
import etl.cmd.SchemaETLCmd;
import etl.engine.LogicSchema;
import etl.engine.types.ProcessMode;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.VarType;
import scala.Tuple2;
import scala.Tuple3;

public abstract class DynamicXmlSchemaCmd extends SchemaETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;
	private static final String CREATE_TABLES_SQL_KEY = "<<create-tables-sql>>";

	public static final Logger logger = LogManager.getLogger(DynamicXmlSchemaCmd.class);
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		if (pm == ProcessMode.Reduce)
			super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm, false);
		else
			super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm, true);
//		this.processType = DynSchemaProcessType.valueOf(super.getCfgString(cfgkey_process_type, DynSchemaProcessType.genCsv.toString()));
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
		
		Map<String, String> attrIdNameMap = logicSchema.getAttrIdNameMap();
		//check new attribute
		List<String> curAttrNames = dt.getFieldNames();
		List<String> curAttrIds = dt.getFieldIds();
		List<String> newAttrNames = new ArrayList<String>();
		List<String> newAttrIds = new ArrayList<String>();
		List<FieldType> newAttrTypes = new ArrayList<FieldType>();
		for (int j=0; j<curAttrNames.size(); j++){//for every attr
			String attrName = curAttrNames.get(j);
			if (orgSchemaAttributes==null || !orgSchemaAttributes.contains(attrName)){
				if(attrIdNameMap.get(curAttrIds.get(j)) != null){ //aleady add the _4_parent field name 
					continue;
				}
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
	
	private UpdatedTable checkSchemaAlterColumn(DynamicTableSchema dt){
		String tableName = dt.getName();
		UpdatedTable ut = new UpdatedTable();
		List<String> orgSchemaAttributes;
		List<FieldType> orgTypes;
		ut.name = tableName;
		ut.id = dt.getId();
		if (logicSchema.hasTable(tableName)){
			orgSchemaAttributes = logicSchema.getAttrNames(tableName);
			orgTypes = logicSchema.getAttrTypes(tableName);
			if(orgSchemaAttributes == null){
				return null;
			}
			//check new attribute
			List<String> curAttrNames = dt.getFieldNames();
			List<String> curAttrIds = dt.getFieldIds();
			List<String> newAttrNames = new ArrayList<String>();
			List<String> newAttrIds = new ArrayList<String>();
			List<FieldType> newAttrTypes = new ArrayList<FieldType>();
			for (int j=0; j<curAttrNames.size(); j++){//for every attr
				String attrName = curAttrNames.get(j);
				int index = orgSchemaAttributes.indexOf(attrName);
				if (index > 0){
					if(orgTypes.get(index).getType() == VarType.NUMERIC){
						FieldType ft = DBUtil.guessDBType(dt.getValueSample()[j]);
						if(ft.getType() != VarType.NUMERIC){
							newAttrNames.add(attrName);
							if (curAttrIds!=null){
								String attrId = curAttrIds.get(j);
								newAttrIds.add(attrId);
							}
							newAttrTypes.add(ft);
						}
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
		return null;

	}
	
	private UpdatedTable checkSchemaRenameColumn(DynamicTableSchema dt){
		String tableName = dt.getName();
		UpdatedTable ut = new UpdatedTable();
		List<String> orgSchemaAttributes;
		List<FieldType> orgTypes;
		ut.name = tableName;
		ut.id = dt.getId();
		if (logicSchema.hasTable(tableName)){
			orgSchemaAttributes = logicSchema.getAttrNames(tableName);
			orgTypes = logicSchema.getAttrTypes(tableName);
			Map<String, String> orgAttrIdNameMap = logicSchema.getAttrIdNameMap();
			if(orgSchemaAttributes == null){
				return null;
			}
			//check new attribute
			List<String> curAttrNames = dt.getFieldNames();
			List<String> curAttrIds = dt.getFieldIds();
			List<String> newAttrNames = new ArrayList<String>();
			List<String> newAttrIds = new ArrayList<String>();
			List<FieldType> newAttrTypes = new ArrayList<FieldType>();
			for (int j=0; j<curAttrNames.size(); j++){//for every attr
				
				String attrName = curAttrNames.get(j);
				String fieldName = orgAttrIdNameMap.get(curAttrIds.get(j));
				if(fieldName != null && fieldName.length() < attrName.length()){
					newAttrNames.add(fieldName);
					newAttrIds.add(curAttrIds.get(j));
					newAttrTypes.add(orgTypes.get(j));
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
		return null;

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
			List<DynamicTableSchema> dtList = getDynamicTable(text, this.logicSchema);
			for(DynamicTableSchema dt:dtList){
				List<String> logInfo = new ArrayList<String>();
				UpdatedTable updateFieldType = checkSchemaAlterColumn(dt); //update the exit fields' type
				if(updateFieldType != null){
					List<String> dropList = removeTableField(updateFieldType.id, updateFieldType.name, updateFieldType.attrIds, updateFieldType.attrNames, updateFieldType.attrTypes);
				    if(dropList != null && dropList.size() > 0){
				    	logInfo.addAll(dropList);
				    }
				}
				UpdatedTable renameFieldName = checkSchemaRenameColumn(dt); //update the field to name_4_parent
				if(renameFieldName != null){
					List<String> dropList = removeTableField(renameFieldName.id, renameFieldName.name, renameFieldName.attrIds, renameFieldName.attrNames, renameFieldName.attrTypes);
				    if(dropList != null && dropList.size() > 0){
				    	logInfo.addAll(dropList);
				    }
				}
				UpdatedTable ut = checkSchemaUpdate(dt);
				if (ut != null) {//needs update
					logger.info(String.format("detect update needed, lock the schema for table %s", dt.getName()));
					// add column
					List<String> uplist = updateSchema(ut.id, ut.name, ut.attrIds, ut.attrNames, ut.attrTypes);
					// atler column type
					if(uplist != null){
						logInfo.addAll(uplist);
					}
					if (logInfo != null && logInfo.size() > 0) {
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
			
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
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
	
	protected static String formatString(String str){
		if(str != null){
			str = str.toLowerCase().replace("-", "_");
			if (Character.isDigit(str.charAt(0))){
				str ="_" + str;
			}
			return str;
		}
		return null;
	}
}
