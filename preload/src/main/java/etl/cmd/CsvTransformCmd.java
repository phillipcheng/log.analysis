package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import etl.cmd.transform.ColOp;
import etl.engine.MRMode;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.ScriptEngineUtil;
import etl.util.Util;
import etl.util.VarType;

public class CsvTransformCmd extends DynaSchemaFileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvTransformCmd.class);
	
	public static final String cfgkey_skip_header="skip.header";
	public static final String cfgkey_row_validation="row.validation";
	public static final String cfgkey_input_endwithcomma="input.endwithcomma";
	public static final String cfgkey_col_op="col.op";
	public static final String cfgkey_old_talbe="old.table";
	public static final String cfgkey_add_fields="add.fields";
	
	private boolean skipHeader=false;
	private String rowValidation;
	private boolean inputEndWithComma=false;
	private List<ColOp> colOpList = new ArrayList<ColOp>();
	private String oldTable;
	private List<String> addFieldsNames = new ArrayList<String>();
	private List<String> addFieldsTypes = new ArrayList<String>();
	
	//
	private List<String> tableAttrs = null;
	private Map<String, Integer> nameIdxMap = null;
	
	
	public CsvTransformCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		skipHeader =pc.getBoolean(cfgkey_skip_header, false);
		rowValidation = pc.getString(cfgkey_row_validation);
		inputEndWithComma = pc.getBoolean(cfgkey_input_endwithcomma, false);
		oldTable = pc.getString(cfgkey_old_talbe, null);
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
		String[] colops = pc.getStringArray(cfgkey_col_op);
		for (String colop:colops){
			ColOp co = new ColOp(colop, nameIdxMap);
			colOpList.add(co);
		}
		String[] nvs = pc.getStringArray(cfgkey_add_fields);
		if (nvs!=null){
			for (String nv:nvs){
				String[] nva = nv.split("\\:");
				addFieldsNames.add(nva[0]);
				addFieldsTypes.add(nva[1]);
			}
		}
		this.setMrMode(MRMode.line);
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

	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		Map<String, Object> retMap = new HashMap<String, Object>();
		String output="";
		
		//process skip header
		if (skipHeader && offset==0) {
			logger.info("skip header:" + row);
			return null;
		}
		
		//get all fiels
		List<String> items = new ArrayList<String>();
		items.addAll(Arrays.asList(row.split(",")));
		
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
		
		//process row validation
		if (rowValidation!=null){
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(ColOp.VAR_NAME_FIELDS, items.toArray());
			boolean valid = (Boolean) ScriptEngineUtil.eval(rowValidation, VarType.BOOLEAN, map);
			if (!valid) {
				logger.info("invalid row:" + row);
				return null;
			}
		}
		
		//process operation
		Map<String, Object> vars = new HashMap<String, Object>();
		String[] strItems = new String[items.size()];
		vars.put(ColOp.VAR_NAME_FIELDS, items.toArray(strItems));
		vars.put(ColOp.VAR_NAME_FIELD_MAP, fieldMap);
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		vars.put(VAR_NAME_FILE_NAME, fileName);
		for (ColOp co: colOpList){
			items = co.process(vars);
			strItems = new String[items.size()];
			vars.put(ColOp.VAR_NAME_FIELDS, items.toArray(strItems));
		}
		output = Util.getCsv(Arrays.asList(strItems), false);
		if (isAddFileName()){
			output+="," + getAbbreFileName(fileName);
		}
		logger.debug("output:" + output);
		retMap.put(RESULT_KEY_OUTPUT, Arrays.asList(new String[]{output}));
		return retMap;
	}
}
