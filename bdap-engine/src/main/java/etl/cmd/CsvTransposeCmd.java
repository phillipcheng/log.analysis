package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class CsvTransposeCmd extends SchemaETLCmd {

	private static final long serialVersionUID = 5412418379859088398L;
	
	private static final Logger logger = LogManager.getLogger(CsvTransposeCmd.class);
	
	//Stor the origin configured table name or splited table name
	public static final String VAR_NAME_ORIGIN_TABLE_NAME="originTableName";
	
	//cfgkey
	public static final String cfgkey_with_trailing_delimiter = "with.trailing.delimiter";	
	
	//Group of columns output
	public static final String cfgkey_group_fields = "group.fields";
	
	//column.name.fields  & column.name.fields appear in pair to indicate which column's value need to transfer to column name and which column's value need to be filled into the new column
	public static final String cfgkey_column_name_fields = "column.name.fields";
	public static final String cfgkey_column_value_fields = "column.value.fields";
	
	//Used only when split.table.fields is not used, to specify the output result table schema
	public static final String cfgkey_table_name = "table.name";
	
	//Use specified fields as table name
	public static final String cfgkey_split_table_fields = "split.table.fields";
	

	public static final String cfgkey_table_name_mapping_exp = "table.name.mapping.exp";

	public static final String cfgkey_table_fields_mapping_exp = "table.fields.mapping.exp";
	
	//used to output the result into different files, if not configuration, all expose into single file
	public static final String cfgkey_output_filename_exp = "output.filename.exp";

	private boolean withTrailingDelimiter = false;
	private List<IdxRange> groupFieldList=new ArrayList<IdxRange>();	
	private String tableName;
	private List<IdxRange> splitTableFieldIdxList=new ArrayList<IdxRange>();	
	private List<Integer> columnNameFieldIdxList=new ArrayList<Integer>();	
	private List<Integer> columnValueFieldIdxList=new ArrayList<Integer>();		
	private boolean isSplitTable=false;

	private CompiledScript outputFileNameCS=null;
	private CompiledScript tableNameMappingCS=null;
	private CompiledScript tableFieldsMappingCS=null;
	
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
		this.withTrailingDelimiter=getCfgBoolean(cfgkey_with_trailing_delimiter, false);
		this.groupFieldList=IdxRange.parseString(getCfgString(cfgkey_group_fields, null));
		
		String outputFilenameExp=super.getCfgString(cfgkey_output_filename_exp, null);
		if(outputFilenameExp!=null){
			outputFileNameCS=ScriptEngineUtil.compileScript(outputFilenameExp);
		}
		
		//Read column name field index
		String[] columnNameFieldIdxArray = super.getCfgStringArray(cfgkey_column_name_fields);
		if (columnNameFieldIdxArray != null) {
			for (String index : columnNameFieldIdxArray) {
				columnNameFieldIdxList.add(Integer.parseInt(index));
			}
		}
		if(columnNameFieldIdxArray==null || columnNameFieldIdxArray.length==0){
			throw new IllegalArgumentException(String.format("column.name.fields parameter is mandatory"));
		}
		
		//Read column value field index
		String[] columnValueFieldIdxArray = super.getCfgStringArray(cfgkey_column_value_fields);
		if (columnValueFieldIdxArray != null) {
			for (String index : columnValueFieldIdxArray) {
				this.columnValueFieldIdxList.add(Integer.parseInt(index));
			}
		}
		if(columnValueFieldIdxArray==null || columnValueFieldIdxArray.length==0){
			throw new IllegalArgumentException(String.format("column.value.fields parameter is mandatory"));
		}
		
		//Validate the size of column.name.fields and column.value.fields must same
		if(columnValueFieldIdxArray.length!=columnNameFieldIdxArray.length){
			throw new IllegalArgumentException(String.format("column.name.fields and column.value.fields' value list must be in pair!"));
		}
		
		//Read column name field table		
		String splitTableFields=getCfgString(cfgkey_split_table_fields, null);
		
		if(splitTableFields!=null){
			this.isSplitTable=true;
			this.splitTableFieldIdxList=IdxRange.parseString(splitTableFields);
		}else{
			this.tableName = super.getCfgString(cfgkey_table_name,null);
			
//			if(this.tableName==null){
//				throw new IllegalArgumentException(String.format("Eithr of column.name.field.tables or split.table.fields is required"));
//			}
			
		}
		
		//Read table name mapping expression
		String tableNameMappingExp=this.getCfgString(cfgkey_table_name_mapping_exp, null);
		if(tableNameMappingExp!=null){
			tableNameMappingCS=ScriptEngineUtil.compileScript(tableNameMappingExp);
		}
		
		//Read table -> fields mapping expression
		String tableFieldsMappingExp = this.getCfgString(cfgkey_table_fields_mapping_exp, null);
		if (tableFieldsMappingExp != null) {
			tableFieldsMappingCS = ScriptEngineUtil.compileScript(tableFieldsMappingExp);
		}else{
			throw new IllegalArgumentException(String.format("table.fields.mapping.exp is mandatory"));
		}
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		//Read filename
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
		
		//Calculate table name
		String originTableName;
		String realTableName;
		if(isSplitTable){
			originTableName=buildString(csvRecord,splitTableFieldIdxList,"_");
		}else{
			originTableName=this.tableName;
		}
		realTableName=originTableName;
		getSystemVariables().put(VAR_NAME_TABLE_NAME, originTableName);
		getSystemVariables().put(VAR_NAME_ORIGIN_TABLE_NAME, originTableName);
		if(tableNameMappingCS!=null){
			realTableName=ScriptEngineUtil.eval(tableNameMappingCS, super.getSystemVariables());
			getSystemVariables().put(VAR_NAME_TABLE_NAME, originTableName);
		}
		
		//Calculate output filename
		String outputFileName=realTableName;
		if(this.outputFileNameCS!=null){
			outputFileName=ScriptEngineUtil.eval(outputFileNameCS, super.getSystemVariables());
		}		
		
		//Get grouped columns
		String groupedColumns=buildString(csvRecord,groupFieldList,",");
		
		//Calculate number of grouped columns
		int numberOfGroupedColumns=0;
		for(IdxRange idxRange:groupFieldList){
			int start=idxRange.getStart();
			int end=idxRange.getEnd();
			if(end==-1){
				end=csvRecord.size()-1;
			}
			numberOfGroupedColumns=numberOfGroupedColumns+(end-start+1);
		}
		
		//Read table field
		List<List<String>> tableFieldGroupList=new ArrayList<List<String>>();
		int numberOfTransposeColumns=0;
		String fieldsStr=ScriptEngineUtil.eval(tableFieldsMappingCS, super.getSystemVariables());
		if(fieldsStr==null || fieldsStr.trim().isEmpty()){
			logger.info("Cannot find transpose columns definition, drop row:{}",row);
			return null;
		}
		
		String[] fieldGroupArray=fieldsStr.split(";");
		if(fieldGroupArray!=null && fieldGroupArray.length>0){
			for(String fieldGroup:fieldGroupArray){
				String[] fieldArray=fieldGroup.split(",");
				if(fieldArray!=null){
					numberOfTransposeColumns=numberOfTransposeColumns+fieldArray.length;
					tableFieldGroupList.add(Arrays.asList(fieldArray));
				}
			}
		}
		
		
		//Transpose
		List<String> transposeList=new ArrayList<String>(numberOfTransposeColumns);
		int fieldGroupIdxOffset=0;
		for(int i=0;i<numberOfTransposeColumns;i++) transposeList.add("");
		for(int idx=0;idx<this.columnNameFieldIdxList.size();idx++){
			int columnNameFieldIdx=this.columnNameFieldIdxList.get(idx);
			int columnValueFieldIdx=this.columnValueFieldIdxList.get(idx);
			//Read & validate column name and value
			if(columnNameFieldIdx>=csvRecord.size()){
				logger.info("The column.name.field:{} is greater than number of record columns:{}, drop row:{}",new Object[]{columnNameFieldIdx, csvRecord.size(), row});
				return null;
			}
			
			if(columnValueFieldIdx>=csvRecord.size()){
				logger.info("The column.value.field:{} is greater than number of record columns:{}, drop row:{}",new Object[]{columnValueFieldIdx, csvRecord.size(), row});
				return null;
			}		
			String colName=csvRecord.get(columnNameFieldIdx);
			String colValue=csvRecord.get(columnValueFieldIdx);
			
			//Locate the column name position in the fields
			int indexOfColName=-1;
			List<String> fieldList=tableFieldGroupList.get(idx);
			for(int i=0;i<fieldList.size();i++){
				if(fieldList.get(i).equalsIgnoreCase(colName)){
					indexOfColName=i;
					break;
				}
			}
			if(indexOfColName==-1){
				logger.info("Cannot find column:{} in table, drop row:{}",colName, row);
				return null;
			}
			
			//Transpose
			transposeList.set(fieldGroupIdxOffset+indexOfColName, colValue);	
			
			fieldGroupIdxOffset=fieldGroupIdxOffset+fieldList.size();
		}		
		
/*		Output 
		key format
		  table name,rows
		value format
		  value1,value2,,,,,valueN
*/	
		context.write(new Text(outputFileName+","+groupedColumns), new Text(String.join(",",transposeList)));
		
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
	public List<String[]> reduceProcess(Text key, Iterable<Text> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
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
}
