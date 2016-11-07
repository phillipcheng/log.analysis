package etl.cmd;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import bdap.util.Util;
import etl.cmd.transform.ColOp;
import etl.engine.ETLCmd;
import etl.engine.MRMode;
import etl.engine.OutputType;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

import scala.Tuple2;

public class CsvTransformCmd extends SchemaFileETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(CsvTransformCmd.class);
	
	//cfgkey
	public static final String cfgkey_skip_header="skip.header";
	public static final String cfgkey_input_endwithcomma="input.endwithcomma";
	public static final String cfgkey_row_validation="row.validation";
	public static final String cfgkey_col_op="col.op";
	public static final String cfgkey_old_talbe="old.table";
	public static final String cfgkey_add_fields="add.fields";
	
	private boolean skipHeader=false;
	private boolean inputEndWithComma=false;
	private String rowValidation;
	private String oldTable;
	
	private transient List<String> addFieldsNames;
	private transient List<String> addFieldsTypes;
	private transient List<ColOp> colOpList;
	
	//
	private transient List<String> tableAttrs = null;
	private transient Map<String, Integer> nameIdxMap = null;
	
	public CsvTransformCmd(){
		super();
	}
	
	public CsvTransformCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvTransformCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		this.setMrMode(MRMode.line);
		skipHeader =super.getCfgBoolean(cfgkey_skip_header, false);
		inputEndWithComma = super.getCfgBoolean(cfgkey_input_endwithcomma, false);
		rowValidation = super.getCfgString(cfgkey_row_validation, null);
		oldTable = super.getCfgString(cfgkey_old_talbe, null);
		addFieldsNames = new ArrayList<String>();
		addFieldsTypes = new ArrayList<String>();
		colOpList = new ArrayList<ColOp>();
		
		//for dynamic trans
		if (this.logicSchema!=null){
			nameIdxMap = new HashMap<String, Integer>();
			tableAttrs = logicSchema.getAttrNames(this.oldTable);
			if (tableAttrs==null){
				logger.error(String.format("table %s not exist or specified.", this.oldTable));
			}else {
				for (int i=0; i<tableAttrs.size(); i++){
					nameIdxMap.put(tableAttrs.get(i), i);
				}
			}
		}
		String[] colops = super.getCfgStringArray(cfgkey_col_op);
		for (String colop:colops){
			ColOp co = new ColOp(colop, nameIdxMap);
			colOpList.add(co);
		}
		String[] nvs = super.getCfgStringArray(cfgkey_add_fields);
		if (nvs!=null){
			for (String nv:nvs){
				String[] nva = nv.split("\\:",2);
				addFieldsNames.add(nva[0]);
				addFieldsTypes.add(nva[1]);
			}
		}
	}
	
	@Override
	public List<String> sgProcess(){
		List<String> attrs = logicSchema.getAttrNames(this.oldTable);
		List<String> createTableSqls = new ArrayList<String>();
		List<String> addFns = new ArrayList<String>();
		List<FieldType> addFts = new ArrayList<FieldType>();
		for (int i=0; i<addFieldsNames.size(); i++){
			String fn = addFieldsNames.get(i);
			FieldType ft = new FieldType(addFieldsTypes.get(i));
			if (!attrs.contains(fn)){
				//update schema
				addFns.add(fn);
				addFts.add(ft);
			}
		}
		if (addFns.size()>0){
			logicSchema.addAttributes(oldTable, addFns);
			logicSchema.addAttrTypes(oldTable, addFts);
			createTableSqls.addAll(DBUtil.genUpdateTableSql(addFns, addFts, oldTable, 
					dbPrefix, super.getDbtype()));
			return super.updateDynSchema(createTableSqls);
		}else{
			return null;
		}
	}

	//value is each line
	public Tuple2<String, String> mapToPair(String pathName, String value){
		super.init();
		this.getSystemVariables().put(VAR_NAME_PATH_NAME, pathName);
		String tableName = getTableName(pathName);
		String row = value;
		String output="";
		
		//get all fiels
		List<String> items = new ArrayList<String>();
		items.addAll(Arrays.asList(row.split(",", -1)));
		
		//set the fieldsMap
		Map<String, String> fieldMap = null;
		if (nameIdxMap!=null){
			fieldMap = new HashMap<String, String>();
			for (int i=0; i<tableAttrs.size(); i++){
				if (i<items.size()){
					fieldMap.put(tableAttrs.get(i), items.get(i));
				}else{
					fieldMap.put(tableAttrs.get(i), "");
				}
			}
		}
		
		//process input ends with comma
		if (inputEndWithComma){//remove the last empty item since row ends with comma
			items.remove(items.size()-1);
		}
		super.getSystemVariables().put(ColOp.VAR_NAME_FIELDS, items.toArray(new String[0]));
		//process row validation
		if (rowValidation!=null){
			boolean valid = (Boolean) ScriptEngineUtil.eval(rowValidation, VarType.BOOLEAN, super.getSystemVariables());
			if (!valid) {
				logger.info("invalid row:" + row);
				return null;
			}
		}
		
		//process operation
		super.getSystemVariables().put(ColOp.VAR_NAME_FIELD_MAP, fieldMap);
		super.getSystemVariables().put(VAR_NAME_TABLE_NAME, tableName);
		for (ColOp co: colOpList){
			items = co.process(super.getSystemVariables());
			super.getSystemVariables().put(ColOp.VAR_NAME_FIELDS, items.toArray(new String[0]));
		}
		output = Util.getCsv(items, false);
		logger.debug("output:" + output);
		return new Tuple2<String, String>(tableName, output);
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) {
		Map<String, Object> retMap = new HashMap<String, Object>();
		//process skip header
		if (skipHeader && offset==0) {
			logger.info("skip header:" + row);
			return null;
		}
		String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
		Tuple2<String, String> ret = mapToPair(pathName, row);
		retMap.put(RESULT_KEY_OUTPUT_TUPLE2, Arrays.asList(ret));
		return retMap;
	}
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values){
		List<String[]> ret = new ArrayList<String[]>();	
		Iterator<Text> it = values.iterator();
		while (it.hasNext()){
			String v = it.next().toString();
			if (super.getOutputType()==OutputType.multiple){
				ret.add(new String[]{v, null, key.toString()});
			}else{
				ret.add(new String[]{v, null, ETLCmd.SINGLE_TABLE});
			}
		}
		return ret;
	}
	
	//key contain fileName, value is each line
	@Override
	public JavaRDD<Tuple2<String, String>> sparkProcessKeyValue(JavaRDD<Tuple2<String, String>> input){
		JavaRDD<Tuple2<String, String>> mapret = input.map(new Function<Tuple2<String, String>, Tuple2<String, String>>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
				return mapToPair(t._1, t._2);
			}
		}).filter(new Function<Tuple2<String, String>, Boolean>(){
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				if (v1==null){
					return false;
				}else{
					return true;
				}
			}
		});
		JavaRDD<Tuple2<String, String>> csvret=mapret;
		if (super.getOutputType()==OutputType.single){
			csvret = mapret.map(new Function<Tuple2<String,String>, Tuple2<String,String>>(){
				@Override
				public Tuple2<String, String> call(Tuple2<String, String> v1) throws Exception {
					return new Tuple2<String,String>(ETLCmd.SINGLE_TABLE, v1._2);
				}
			});
		}
		
		return csvret;
	}

	public String getOldTable() {
		return oldTable;
	}

	public void setOldTable(String oldTable) {
		this.oldTable = oldTable;
	}
}
