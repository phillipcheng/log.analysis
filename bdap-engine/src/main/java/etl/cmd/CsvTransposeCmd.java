package etl.cmd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.HdfsUtil;
import etl.cmd.transform.ColOp;
import etl.engine.ETLCmd;
import etl.engine.MRMode;
import etl.engine.OutputType;
import etl.util.IdxRange;
import etl.util.ScriptEngineUtil;

/*
 * type, column value/index
 * 
 * a set of transpose column's value is the type. 
 * 
 * typeA:{{S1:1}, {S2:2}}
 * 
 *  Name,Subject,Grade               Name,S1,S2
 *  -------------------              ------------- 
 *  N1  , S1    , A           ==>    N1  ,A ,B
 *  N1  , S2    , B
 */

public class CsvTransposeCmd extends SchemaFileETLCmd {

	private static final long serialVersionUID = 5412418379859088398L;
	
	private static final Logger logger = LogManager.getLogger(CsvTransposeCmd.class);
	
	private static final String VAR_NAME_SPLITED_TABLE_NAME="splitedTableName";
	
	//cfgkey	
	public static final String cfgkey_skip_header = "skip.header";
	public static final String cfgkey_with_trailing_delimiter = "with.trailing.delimiter";	
	public static final String cfgkey_transpose_schema_file = "transpose.schema.file";	
	
	//Besides the row transpose to column, output specified raw group columns 
	public static final String cfgkey_rows_fields = "row.fields";
	
	//field.name.keys, field.value.keys & field.name.keys.schema.type they are 1:1:1 configured
	//define which columns value will transpose to field name
	public static final String cfgkey_column_name_fields = "column.name.fields";
	//define how many possible field names
	public static final String cfgkey_column_name_field_schema_types = "column.name.field.schema.types";
	//define which columns value will transpose to transposed field's value
	public static final String cfgkey_column_value_field = "column.value.fields";
	
	//used for the mapping find table type list
	public static final String cfgkey_split_table_fields = "split.table.fields";
	
	//used to output the result into different files, if not configuration, all expose into single file
	public static final String cfgkey_table_name_exp = "table.name.exp";

	private boolean skipHeader = false;
	private boolean withTrailingDelimiter = false;
	private List<IdxRange> rowFieldList=new ArrayList<IdxRange>();
	private List<IdxRange> splitTableFieldList=new ArrayList<IdxRange>();
	private List<Integer> columnNameFieldList=new ArrayList<Integer>();
	private List<String> noSplitedTypeColumnNameFieldSchemaTypeList=new ArrayList<String>();
	private List<Integer> columnValueFieldList=new ArrayList<Integer>();	
	private CompiledScript tableNameCS=null;
	private TransposeSchema transposeSchema=null;
	
	public CsvTransposeCmd(){
		super();
	}
	
	public CsvTransposeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvTransposeCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		setMrMode(MRMode.line);
		this.skipHeader =getCfgBoolean(cfgkey_skip_header, false);
		this.withTrailingDelimiter=getCfgBoolean(cfgkey_with_trailing_delimiter, false);
		this.rowFieldList=IdxRange.parseString(getCfgString(cfgkey_rows_fields, null));
		this.splitTableFieldList=IdxRange.parseString(getCfgString(cfgkey_split_table_fields, null));
		
		String transposeSchemaFile=getCfgString(cfgkey_transpose_schema_file, null);
		transposeSchema=loadTransposeSchema(transposeSchemaFile);		
		logger.info("Transpose Schema:{}", transposeSchema);
		
		String tableNameExp=super.getCfgString(cfgkey_table_name_exp, null);
		if(tableNameExp!=null){
			tableNameCS=ScriptEngineUtil.compileScript(tableNameExp);
		}	
		
		//Read field name keys
		String[] columnNameFieldsArray=super.getCfgStringArray(cfgkey_column_name_fields);
		if(columnNameFieldsArray!=null){
			for (String columnNameField :columnNameFieldsArray) {
				this.columnNameFieldList.add(Integer.parseInt(columnNameField));
			}
		}		
		
		//Read field name key schema types
		String[] columnNameFieldSchemaTypesArray=super.getCfgStringArray(cfgkey_column_name_field_schema_types);
		if(columnNameFieldSchemaTypesArray!=null){
			this.noSplitedTypeColumnNameFieldSchemaTypeList.addAll(Arrays.asList(columnNameFieldSchemaTypesArray));
		}
		
		//Read field value keys
		String[] columnValueFieldsArray=super.getCfgStringArray(cfgkey_column_value_field);
		if(columnValueFieldsArray!=null){
			for (String columnValueField :columnValueFieldsArray) {
				this.columnValueFieldList.add(Integer.parseInt(columnValueField));
			}
		}
		
//		if(columnNameFieldList.size()==0 || columnValueFieldList.size()==0 || noSplitedTypeColumnNameFieldSchemaTypeList.size()==0){
//			throw new IllegalArgumentException(String.format("field name keys, field values and field name key schema type are mandatory"));
//		}
		
//		
//		if ((columnNameFieldList.size()!= columnValueFieldList.size()) ||  (columnNameFieldList.size()!= noSplitedTypeColumnNameFieldSchemaTypeList.size())){
//			throw new IllegalArgumentException(String.format("The length among field name keys, field values and field name key schema type must be same"));
//		}
	
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		//Read file name
		String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		getSystemVariables().put(VAR_NAME_FILE_NAME, inputFileName);
		
		//Skip header
		if (skipHeader && offset == 0) {
			logger.info("skip header:" + row);
			return null;
		}
		
		//Parse input
		CSVParser csvParser=CSVParser.parse(row, CSVFormat.DEFAULT.withTrim().withTrailingDelimiter(withTrailingDelimiter));
		CSVRecord csvRecord=csvParser.getRecords().get(0);
		String[] fields=new String[csvRecord.size()];
		for(int i=0;i<csvRecord.size();i++){
			fields[i]=csvRecord.get(i);
		}
		getSystemVariables().put(ColOp.VAR_NAME_FIELDS, fields);
		
		//Get table name
		String splitedTableName=buildString(csvRecord,splitTableFieldList,"_");
		getSystemVariables().put(VAR_NAME_SPLITED_TABLE_NAME, splitedTableName);
		String tableName=splitedTableName;
		if(this.tableNameCS!=null){
			tableName=ScriptEngineUtil.eval(tableNameCS, super.getSystemVariables());
		}		
		
		//Get row Keys
		String rows=buildString(csvRecord,rowFieldList,",");
		
		//Transpose
		List<String> columnNameFieldSchemaTypeList=noSplitedTypeColumnNameFieldSchemaTypeList;
		if(this.splitTableFieldList!=null && splitTableFieldList.size()>0){
			columnNameFieldSchemaTypeList=this.transposeSchema.getTableMap().get(splitedTableName);
		}
		
		if(columnNameFieldSchemaTypeList==null || columnNameFieldSchemaTypeList.isEmpty()){
			logger.info("Column name field schema type is not defined, skip row:{}", row);
			return null;
		}			
		
		List<String> transposeList=new ArrayList<String>();
		for(int idx=0;idx<columnNameFieldSchemaTypeList.size();idx++){
			//build new columns
			List<String> colNameList=this.transposeSchema.getTypeMap().get(columnNameFieldSchemaTypeList.get(idx));	
			List<String> subTransposeList=new ArrayList<String>(colNameList.size());
			for(int i=0;i<colNameList.size();i++) subTransposeList.add("");
			
			//values to specified location of columns
			String colName=csvRecord.get(columnNameFieldList.get(idx));
			String colValue=csvRecord.get(columnValueFieldList.get(idx));
			
			int indexOfColName=colNameList.indexOf(colName);
			if(indexOfColName==-1){
				logger.warn("Drop row:{} as cannot find releated column name in schema", row);
				continue;
			}			
			subTransposeList.set(indexOfColName, colValue);
			
			//merge the schema type into full transposeList
			transposeList.addAll(subTransposeList);
		}
		
		//key format
		//  talbename,rows
		//value format
		//  schemaType1Value1, schemaType1Value2,,,,,schemaType1Valuen,schemaType2Value1, schemaType2Value2,,,,,schemaType2Valuen
		context.write(new Text(tableName+","+rows), new Text(String.join(",",transposeList)));
		
		return null;
	}
	
	public String buildString(CSVRecord csvRecord,List<IdxRange> idxRangeList, String delimiter){
		StringBuilder sb=new StringBuilder();
		for(IdxRange idx:idxRangeList){
			int end=idx.getEnd();
			if(end==-1) end=csvRecord.size()-1;
			for(int i=idx.getStart();i<=idx.getEnd();i++){
				sb.append(csvRecord.get(i)).append(delimiter);
			}
		}
		
		if(sb.length()>0){
			sb.setLength(sb.length()-delimiter.length());
		}
		
		return sb.toString();
	}
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values) throws Exception{
		List<String[]> ret = new ArrayList<String[]>();	
		
		List<String> recordMerged=null;
		for(Text value:values){
			CSVParser csvParser=CSVParser.parse(value.toString(), CSVFormat.DEFAULT.withTrim().withTrailingDelimiter(false));
			CSVRecord csvRecord=csvParser.getRecords().get(0);
			if(recordMerged==null){
				recordMerged=new ArrayList<String>();
				for(String field:csvRecord){
					recordMerged.add(field);
				}
			}else{
				for(int idx=0;idx<recordMerged.size();idx++){
					if(recordMerged.get(idx).isEmpty() && !csvRecord.get(idx).isEmpty()){
						recordMerged.set(idx, csvRecord.get(idx));
					}					
				}
			}
		}
		
		String[] keys=key.toString().split(",");
		String tableName=keys[0];
		String[] keysWithoutTableName=new String[keys.length-1];
		
		System.arraycopy(keys, 1, keysWithoutTableName, 0, keys.length-1);
		String keyValue=String.join(",",keysWithoutTableName);
		
		if (super.getOutputType()==OutputType.multiple){
			ret.add(new String[]{keyValue, String.join(",", recordMerged), tableName});
		}else{
			ret.add(new String[]{keyValue, String.join(",", recordMerged), ETLCmd.SINGLE_TABLE});
		}
		return ret;
	}
	
	private TransposeSchema loadTransposeSchema(String transposeSchemaFile){
		Path schemaFilePath = new Path(transposeSchemaFile);
		try {
			if (fs.exists(schemaFilePath)){
				return (TransposeSchema) HdfsUtil.fromDfsJsonFile(fs, transposeSchemaFile, TransposeSchema.class);
			}else{
				return new TransposeSchema();
			}
		} catch (IOException e) {
			logger.error("", e);
			return new TransposeSchema();
		}
	}
	
	/**
	 * 
	 * @author zhuxiang
	 *  
	 */
	public static class TransposeSchema{
		//table name and the column name field schema type mapping
		private Map<String, List<String>> tableMap;
		
		//type definition
		private Map<String, List<String>> typeMap;
		
		
		public Map<String, List<String>> getTableMap() {
			return tableMap;
		}
		public void setTableMap(Map<String, List<String>> tableMap) {
			this.tableMap = tableMap;
		}
		
		public Map<String, List<String>> getTypeMap() {
			return typeMap;
		}
		public void setTypeMap(Map<String, List<String>> typeMap) {
			this.typeMap = typeMap;
		}
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("TransposeSchema [tableMap=");
			builder.append(tableMap);
			builder.append(", typeMap=");
			builder.append(typeMap);
			builder.append("]");
			return builder.toString();
		}
	}
}
