package etl.cmd.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import etl.engine.AggrOperator;
import etl.engine.DynaSchemaFileETLCmd;
import etl.util.DBUtil;
import etl.util.IdxRange;
import etl.util.Util;

public class CsvAggregateCmd extends DynaSchemaFileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvAggregateCmd.class);

	public static final String AGGR_OP_SUM="sum";
	public static final String AGGR_OPERATOR_SEP="\\|";
	
	public static final String cfgkey_aggr_op="aggr.op";
	public static final String cfgkey_aggr_groupkey="aggr.groupkey";
	public static final String cfgkey_aggr_old_table="old.table";
	public static final String cfgkey_aggr_new_table="new.table";
	
	
	private String[] oldTables = null;
	private String[] newTables = null;
	private Map<String, AggrOpMap> aoMapMap = new HashMap<String, AggrOpMap>();
	private Map<String, List<IdxRange>> groupKeysMap = new HashMap<String, List<IdxRange>>();
	
	public CsvAggregateCmd(String wfid, String staticCfg, String dynCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, dynCfg, defaultFs, otherArgs);
		if (pc.containsKey(cfgkey_aggr_old_table)){
			oldTables = pc.getStringArray(cfgkey_aggr_old_table);
		}
		if (pc.containsKey(cfgkey_aggr_new_table)){
			newTables = pc.getStringArray(cfgkey_aggr_new_table);
		}
		if (oldTables==null){
			String[] strAggrOpList = pc.getStringArray(cfgkey_aggr_op);
			AggrOpMap aoMap = new AggrOpMap(strAggrOpList);
			aoMapMap.put(SINGLE_TABLE, aoMap);
			List<IdxRange> groupKeys = IdxRange.parseString(pc.getString(cfgkey_aggr_groupkey));
			groupKeysMap.put(SINGLE_TABLE, groupKeys);
		}else{
			for (String tableName: oldTables){
				String[] strAggrOpList = pc.getStringArray(tableName+"."+cfgkey_aggr_op);
				AggrOpMap aoMap = new AggrOpMap(strAggrOpList);
				aoMapMap.put(tableName, aoMap);
				List<IdxRange> groupKeys = IdxRange.parseString(pc.getString(tableName+"."+cfgkey_aggr_groupkey));
				groupKeysMap.put(tableName, groupKeys);
			}
		}
	}
	
	private List<String> getCsvFields(CSVRecord r, List<IdxRange> irl){
		List<String> keys = new ArrayList<String>();
		for (IdxRange ir: irl){
			int start = ir.getStart();
			int end = ir.getEnd();
			if (ir.getEnd()==-1){
				end = r.size()-1;
			}
			for (int i=start; i<=end; i++){
				keys.add(r.get(i));
			}
		}
		return keys;
	}
	
	//single process for schema update
	@Override
	public List<String> sgProcess(){
		boolean schemaUpdated = false;
		List<String> createTableSqls = new ArrayList<String>();
		for (int tbIdx=0; tbIdx<oldTables.length; tbIdx++){
			String oldTable = oldTables[tbIdx];
			String newTable = newTables[tbIdx];
			List<String> attrs = logicSchema.getAttrNames(oldTable);
			List<String> attrTypes = logicSchema.getAttrTypes(oldTable);
			int idxMax = attrs.size()-1;
			List<String> newAttrs = new ArrayList<String>();
			List<String> newTypes = new ArrayList<String>();
			List<IdxRange> groupKeys = groupKeysMap.get(oldTable);
			AggrOp aop = new AggrOp(AggrOperator.group, groupKeys);
			AggrOpMap aoMap = aoMapMap.get(oldTable);
			aoMap.addAggrOp(aop);
			aoMap.constructMap(idxMax);
			for (int i=0; i<=idxMax; i++){
				if (aoMap.getOp(i)!=null){
					newAttrs.add(attrs.get(i));
					newTypes.add(attrTypes.get(i));
				}
			}
			if (!logicSchema.hasTable(newTable)){
				//update schema
				logicSchema.updateTableAttrs(newTable, newAttrs);
				logicSchema.updateTableAttrTypes(newTable, attrTypes);
				schemaUpdated = true;
				//generate create table
				createTableSqls.add(DBUtil.genCreateTableSql(newAttrs, newTypes, newTable, dbPrefix));
			}else{
				List<String> existNewAttrs = logicSchema.getAttrNames(newTable);
				List<String> existNewAttrTypes = logicSchema.getAttrTypes(newTable);
				if (existNewAttrs.containsAll(newAttrs)){//
					//do nothing
				}else{
					//update schema, happens only when the schema is updated by external force
					logicSchema.updateTableAttrs(newTable, newAttrs);
					logicSchema.updateTableAttrTypes(newTable, newTypes);
					schemaUpdated = true;
					//generate alter table
					newAttrs.removeAll(existNewAttrs);
					newTypes.removeAll(existNewAttrTypes);
					createTableSqls.addAll(DBUtil.genUpdateTableSql(newAttrs, newTypes, newTable, dbPrefix));
				}
			}
		}
		return super.updateDynSchema(createTableSqls, schemaUpdated, Arrays.asList(newTables));
	}

	@Override
	public Map<String, String> reduceMapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		try {
			String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT);
			List<CSVRecord> csvl = parser.getRecords();
			String tableKey = SINGLE_TABLE;
			for (CSVRecord r: csvl){
				List<IdxRange> groupKeys = null;
				if (oldTables==null){
					groupKeys = groupKeysMap.get(SINGLE_TABLE);
				}else{
					String fileKey = inputFileName;
					if (inputFileName.lastIndexOf(".")!=-1){//remove suffix
						fileKey = inputFileName.substring(0, inputFileName.lastIndexOf("."));
					}
					if (groupKeysMap.containsKey(fileKey)){
						tableKey = fileKey;
						groupKeys = groupKeysMap.get(fileKey);
					}else{
						logger.debug(String.format("groupKeysMap %s does not have inputFileName %s", groupKeysMap.keySet(), fileKey));
						break;
					}
				}
				List<String> keys = getCsvFields(r, groupKeys);
				keys.add(0, tableKey);
				String newKey = Util.getCsv(keys, false);
				logger.debug(String.format("new key:%s", newKey));
				context.write(new Text(newKey), new Text(row));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
	
	
	@Override
	public String[] reduceProcess(Text key, Iterable<Text> values){
		String[] kl = key.toString().split(KEY_SEP);
		String tableName = kl[0];
		List<String> ks = new ArrayList<String>();
		for (int i=1; i<kl.length; i++){
			ks.add(kl[i]);
		}
		String newKey = String.join(KEY_SEP, ks);
		List<CSVRecord> rl = new ArrayList<CSVRecord>();
		for (Text v: values){
			try {
				CSVParser parser = CSVParser.parse(v.toString(), CSVFormat.DEFAULT);
				rl.addAll(parser.getRecords());
			}catch(Exception e){
				logger.error("", e);
			}
		}
		int idxMax=rl.get(0).size()-1;
		AggrOpMap aoMap = aoMapMap.get(tableName);
		aoMap.constructMap(idxMax);
		List<String> aggrValues = new ArrayList<String>();
		for (int i=0; i<=idxMax; i++){
			AggrOperator op = aoMap.getOp(i);
			if (op!=null){
				if (AggrOperator.sum==op){
					float av=0;
					for (CSVRecord r:rl){
						String strv = r.get(i);
						float v =0f;
						if (!"".equals(strv.trim())){
							v= Float.parseFloat(strv);		
						}
						av +=v;
					}
					aggrValues.add(Float.toString(av));
				}else{
					logger.error(String.format("op %s not supported yet.", op.toString()));
				}
			}
		}
		
		return new String[]{newKey, Util.getCsv(aggrValues, false), tableName};
	}
}