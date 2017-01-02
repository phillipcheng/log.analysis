package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.IdxRange;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;
import scala.Tuple3;

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
	
	//Store the origin configured table name or splited table name
	public static final String VAR_NAME_ORIGIN_TABLE_NAME="originTableName";	
	//the table.field's name in logic schema
	public static final String VAR_NAME_FIELD_NAME="fieldName";
	
	//cfgkey
	public static final @ConfigKey(type=Boolean.class) String cfgkey_with_trailing_delimiter = "with.trailing.delimiter";	
	
	//Group of columns output
	public static final @ConfigKey String cfgkey_group_fields = "group.fields";
	
	//column.name.fields  & column.name.fields appear in pair to indicate which column's value need to transfer to column name and which column's value need to be filled into the new column
	public static final @ConfigKey(type=String[].class) String cfgkey_column_name_fields = "column.name.fields";
	public static final @ConfigKey(type=String[].class) String cfgkey_column_value_fields = "column.value.fields";
	
	//Used only when split.table.fields is not used, to specify the output result table schema
	public static final @ConfigKey String cfgkey_table_name = "table.name";
	
	//Use specified fields as table name, it must be subset of group.fields
	public static final @ConfigKey String cfgkey_split_table_fields = "split.table.fields";
	
	public static final @ConfigKey String cfgkey_table_name_mapping_exp = "table.name.mapping.exp";

	//according to it, mapping schema field name, to column.name
	public static final @ConfigKey String cfgkey_field_name_mapping_exp = "field.name.mapping.exp";
	
	//To indicate since where position of the table to start the transpose work. E.g. 
	public static final @ConfigKey(type=Integer.class) String cfgkey_table_field_transpose_start_index = "table.field.transpose.start.index";
	
	//used to output the result into different files, if not configuration, all expose into single file
	public static final @ConfigKey String cfgkey_output_filename_exp = "output.filename.exp";

	private boolean withTrailingDelimiter = false;
	private List<IdxRange> groupFieldList=new ArrayList<IdxRange>();	
	private String tableName;
	private List<IdxRange> splitTableFieldIdxList=new ArrayList<IdxRange>();	
	private List<Integer> columnNameFieldIdxList=new ArrayList<Integer>();	
	private List<Integer> columnValueFieldIdxList=new ArrayList<Integer>();		
	private boolean isSplitTable=false;
	private int tableFieldTransposeStartIndex=0;

	private transient CompiledScript outputFileNameCS=null;
	private transient CompiledScript tableNameMappingCS=null;
	private transient CompiledScript fieldNameMappingCS=null;
	
	private Map<String,Map<String,Integer>> transposeFieldLocationList;
	
	public CsvTransposeCmd(){
		super();
	}
	
	public CsvTransposeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public CsvTransposeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public CsvTransposeCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		setMrMode(MRMode.line);
		this.withTrailingDelimiter=getCfgBoolean(cfgkey_with_trailing_delimiter, false);
		this.groupFieldList=IdxRange.parseString(getCfgString(cfgkey_group_fields, null));
		if(super.getCfgString(cfgkey_table_field_transpose_start_index, null)==null){
			throw new IllegalArgumentException(String.format("table.field.transpose.start.index is mandatory"));			
		}
		this.tableFieldTransposeStartIndex=super.getCfgInt(cfgkey_table_field_transpose_start_index, 0);
		
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
		if(columnNameFieldIdxArray.length>1){
			throw new IllegalArgumentException(String.format("column.name.fields only accept 1 parameter"));
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
		if(columnValueFieldIdxArray.length>1){
			throw new IllegalArgumentException(String.format("column.value.fields only accept 1 parameter"));
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
			
			if(this.tableName==null){
				throw new IllegalArgumentException(String.format("Either of column.name.field.tables or split.table.fields is required"));
			}
		}
		
		//Read table name mapping expression
		String tableNameMappingExp=this.getCfgString(cfgkey_table_name_mapping_exp, null);
		if(tableNameMappingExp!=null){
			tableNameMappingCS=ScriptEngineUtil.compileScript(tableNameMappingExp);
		}
		
		//Read field name mapping expression and construct index
		String fieldNameMappingExp = this.getCfgString(cfgkey_field_name_mapping_exp, null);
		if (fieldNameMappingExp != null) {
			this.fieldNameMappingCS = ScriptEngineUtil.compileScript(fieldNameMappingExp);
		}
		
		transposeFieldLocationList=new ConcurrentHashMap<String,Map<String,Integer>>();		
		for(String tn:logicSchema.getAttrNameMap().keySet()){
			List<String> attrNameList=logicSchema.getAttrNames(tn);
			Map<String,Integer> fieldPosition=transposeFieldLocationList.get(tn);
			if(fieldPosition==null){
				fieldPosition=new ConcurrentHashMap<String,Integer>();
				transposeFieldLocationList.put(tn, fieldPosition);
			}
			
			for(int idx=tableFieldTransposeStartIndex;idx<attrNameList.size();idx++){
				if(fieldNameMappingCS!=null){
					getSystemVariables().put(VAR_NAME_FIELD_NAME, attrNameList.get(idx));
					String mappedFieldName=ScriptEngineUtil.eval(fieldNameMappingCS, getSystemVariables());
					fieldPosition.put(mappedFieldName, idx-tableFieldTransposeStartIndex);
				}else{
					fieldPosition.put(attrNameList.get(idx), idx-tableFieldTransposeStartIndex);
				}
				
			}
		}
	}
	
	private String buildString(CSVRecord csvRecord,List<IdxRange> idxRangeList, String delimiter){
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
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception{
		super.init();
		//Read filename
		getSystemVariables().put(VAR_NAME_FILE_NAME, tableName);
		
		//Parse input
		CSVParser csvParser=CSVParser.parse(value, CSVFormat.DEFAULT.withTrim().withTrailingDelimiter(withTrailingDelimiter));
		CSVRecord csvRecord=csvParser.getRecords().get(0);
		String[] fields=new String[csvRecord.size()];
		for(int i=0;i<csvRecord.size();i++){
			fields[i]=csvRecord.get(i);
		}
		getSystemVariables().put(ColOp.VAR_NAME_FIELDS, fields);
		
		//Calculate table name,the realTableName is used to find in the schema file.
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
		
		//Transpose Preparation
		if(!logicSchema.hasTable(realTableName)){
			logger.warn("Table:{} cannot found in schema, drop row:{}", realTableName, value);
			return null;
		}
		List<String> fieldNameList=logicSchema.getAttrNames(realTableName);
		int transposeSize=fieldNameList.size()-tableFieldTransposeStartIndex;
		if(transposeSize<0){
			logger.warn("Table:{} field transpose start index out of bound, drop row:{}", realTableName, value);
			return null;
		}
		List<String> transposeList=new ArrayList<String>(transposeSize);
		for(int idx=0;idx<transposeSize;idx++) transposeList.add("");
		
		Map<String,Integer> transposeFieldLocation=transposeFieldLocationList.get(realTableName);
		
		for(int idx=0;idx<this.columnNameFieldIdxList.size();idx++){
			int columnNameFieldIdx=this.columnNameFieldIdxList.get(idx);
			int columnValueFieldIdx=this.columnValueFieldIdxList.get(idx);
			//Read & validate column name and value
			if(columnNameFieldIdx>=csvRecord.size()){
				logger.info("The column.name.field:{} is greater than number of record columns:{}, drop row:{}",
						new Object[]{columnNameFieldIdx, csvRecord.size(), value});
				return null;
			}
			
			if(columnValueFieldIdx>=csvRecord.size()){
				logger.info("The column.value.field:{} is greater than number of record columns:{}, drop row:{}",
						new Object[]{columnValueFieldIdx, csvRecord.size(), value});
				return null;
			}		
			String colName=csvRecord.get(columnNameFieldIdx);
			String colValue=csvRecord.get(columnValueFieldIdx);
			
			//Locate the column name position in the fields
			Integer indexOfColName=transposeFieldLocation.get(colName);			

			if(indexOfColName==null){
				logger.info("Cannot find column:{} in table, drop row:{}", colName, value);
				return null;
			}
			
			//Transpose
			transposeList.set(indexOfColName, colValue);	
		}
		//key:table name,rows   value:value1,value2,,,,,valueN
		String outputKey= outputFileName+","+groupedColumns;
		String outputValue = String.join(",",transposeList);
		if (context!=null){
			context.write(new Text(outputKey), new Text(outputValue));
			return null;
		}else{
			return Arrays.asList(new Tuple2<String, String>(outputKey, outputValue));
		}
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		//Read filename
		String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//Skip header
		if (skipHeader && offset == 0) {
			logger.info("skip header:" + row);
			return null;
		}
		this.flatMapToPair(inputFileName, row, context);
		return null;
	}
	
	@Override
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		List<Tuple3<String, String, String>> ret = new ArrayList<Tuple3<String,String,String>>();
		List<String> recordMerged=null;
		for(String value:values){
			CSVParser csvParser=CSVParser.parse(value, CSVFormat.DEFAULT.withTrim().withTrailingDelimiter(false));
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
			ret.add(new Tuple3<String,String,String>(keyValue, String.join(",", recordMerged), tableName));
		}else{
			ret.add(new Tuple3<String,String,String>(keyValue, String.join(",", recordMerged), ETLCmd.SINGLE_TABLE));
		}
		return ret;
	}
	
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
