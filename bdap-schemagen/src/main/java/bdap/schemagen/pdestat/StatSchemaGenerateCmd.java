package bdap.schemagen.pdestat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.HdfsUtil;
import etl.cmd.SchemaETLCmd;
import etl.engine.types.ProcessMode;
import etl.util.DBUtil;
import etl.util.FieldType;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class StatSchemaGenerateCmd extends SchemaETLCmd {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(StatSchemaGenerateCmd.class);
	
	public static final String cfgkey_incoming_folder="incoming.folder";
	public static final String cfgkey_table_name_prefix="table.name.prefix";
	public static final String cfgkey_output_transpose_file="output.transpose.file";
	public static final String cfgkey_output_sql_file="output.sql.file";
	
	
	private String incomingFolder;
	private String tableNamePrefix;
	private String outputTransposeFile;
	private String outputSqlFile;
	
	private Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' name (compared with the org schema)
	private Map<String, List<FieldType>> schemaAttrTypeUpdates = new HashMap<String, List<FieldType>>();//store updated/new tables' attribute parts' type (compared with the org schema)
	private Map<String, List<String>> newTableObjNamesAdded = new HashMap<String, List<String>>();//store new tables' obj name
	private Map<String, List<FieldType>> newTableObjTypesAdded = new HashMap<String, List<FieldType>>();//store new tables' obj type
	
	public StatSchemaGenerateCmd(){
		super();
	}
	
	public StatSchemaGenerateCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		String incomingFolderExp = super.getCfgString(cfgkey_incoming_folder, null);
		if (incomingFolderExp!=null){
			this.incomingFolder = (String) ScriptEngineUtil.eval(incomingFolderExp, VarType.STRING, super.getSystemVariables());
		}
		
		tableNamePrefix=super.getCfgString(cfgkey_table_name_prefix,null);
		outputTransposeFile=super.getCfgString(cfgkey_output_transpose_file,null);
		outputSqlFile=super.getCfgString(cfgkey_output_sql_file,null);
	}
	
	private String readRegNumDesc(CSVRecord record){
		String regNumDesc=record.get(6);
		for(int i=7;i<record.size();i++){
			regNumDesc=regNumDesc+","+record.get(i);
		}
		return regNumDesc;
	}
	
	private boolean checkSchemaUpdate(FileStatus[] inputFileNames) throws Exception {
		boolean schemaUpdated = false;

		for (FileStatus inputFile : inputFileNames) {
			logger.debug(String.format("process %s", inputFile));
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputFile.getPath())));

			String line = br.readLine();
			while (line != null) {
				CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT.withTrim().withTrailingDelimiter(true));
				List<CSVRecord> records=parser.getRecords();
				if(records.size()==0){
					line=br.readLine();
					continue;
				}
				CSVRecord record = records.get(0);
				if(record.size()<7){
					line=br.readLine();
					continue;
				}

				String groupId = record.get(2);
				String regNum = record.get(3);
				String regNumDesc = readRegNumDesc(record).toUpperCase();

				// table Name
				String tableName = tableNamePrefix + groupId;

				// field Name
				String fieldName = String.format("_%s_%s", regNum, regNumDesc.replaceAll("\\s+", "_").replaceAll("_+", "_").replaceAll("[^0-9a-zA-Z_]", ""));

				if (!logicSchema.hasTable(tableName)) {
					// new table
					List<String> attrNameList = newTableObjNamesAdded.get(tableName);
					List<FieldType> attrTypeList = newTableObjTypesAdded.get(tableName);
					if (attrNameList == null) {
						// Add fixed columns
						attrNameList = new ArrayList<String>();
						newTableObjNamesAdded.put(tableName, attrNameList);
						attrNameList.add("COLLECT_DATETIME");
						attrNameList.add("SITE");
						attrNameList.add("CPUID");

						attrTypeList = new ArrayList<FieldType>();
						newTableObjTypesAdded.put(tableName, attrTypeList);
						attrTypeList.add(new FieldType("timestamp"));
						attrTypeList.add(new FieldType("varchar(50)"));
						attrTypeList.add(new FieldType("varchar(50)"));
					}

					if (!checkDuplicatedRegNum(regNum, attrNameList)) {
						attrNameList.add(fieldName);
						attrTypeList.add(new FieldType("numeric(30,2)"));
					}
					
					schemaUpdated=true;

				} else {
					// update table
					List<String> attrNameList = schemaAttrNameUpdates.get(tableName);
					List<FieldType> attrTypeList = schemaAttrTypeUpdates.get(tableName);

					if (attrNameList == null) {
						attrNameList = new ArrayList<String>();
						schemaAttrNameUpdates.put(tableName, attrNameList);
						attrTypeList = new ArrayList<FieldType>();
						schemaAttrTypeUpdates.put(tableName, attrTypeList);
					}

					if (!checkDuplicatedRegNum(regNum, logicSchema.getAttrNames(tableName))) {
						if (!checkDuplicatedRegNum(regNum, attrNameList)) {
							attrNameList.add(fieldName);
							attrTypeList.add(new FieldType("numeric(30,2)"));
							schemaUpdated=true;
						}
					}
				}
				
				line=br.readLine();
			}
			
			br.close();
		}

		return schemaUpdated;
	}
	
	private boolean checkDuplicatedRegNum(String regNum,List<String> attrNameList){
		if(attrNameList!=null){
			for(String field:attrNameList){
				if(field.startsWith("_"+regNum)) return true;
			}
		}
		return false;
	}
	
	/*
	 * output: schema, createsql, transpose mapping
	*/
	@Override
	public List<String> sgProcess() {
		List<String> logInfo = new ArrayList<String>();
		try {
			FileStatus[] inputFileNames = fs.listStatus(new Path(incomingFolder));
			logInfo.add(inputFileNames.length + "");//number of input files
			boolean schemaUpdated = checkSchemaUpdate(inputFileNames);
			if (schemaUpdated){//updated
				logger.info("Schema Changed");
				List<String> createTableSqls = new ArrayList<String>();

				for(String tn:schemaAttrNameUpdates.keySet()){
					//update schema
					logicSchema.addAttributes(tn, schemaAttrNameUpdates.get(tn));
					logicSchema.addAttrTypes(tn, schemaAttrTypeUpdates.get(tn));
					//gen update sql
					createTableSqls.addAll(DBUtil.genUpdateTableSql(schemaAttrNameUpdates.get(tn), schemaAttrTypeUpdates.get(tn), tn, dbPrefix, super.getDbtype()));
				}
				
				for(String tn:newTableObjNamesAdded.keySet()){
					//update to logic schema
					logicSchema.updateTableAttrs(tn, newTableObjNamesAdded.get(tn));
					logicSchema.updateTableAttrTypes(tn, newTableObjTypesAdded.get(tn));
					//gen update sql
					createTableSqls.add(DBUtil.genCreateTableSql(newTableObjNamesAdded.get(tn), newTableObjTypesAdded.get(tn), 
							tn, 	dbPrefix, super.getDbtype(), super.getStoreFormat()));
				}
				
				
				
				//refresh transpose mapping
				String transposeMapping=buildTransposeMapping();
				HdfsUtil.writeDfsFile(getFs(),this.outputTransposeFile, transposeMapping.getBytes());
				
//				logger.info("Transpose Mapping:\n{}", transposeMapping);
				//update logic schema file
				HdfsUtil.toDfsJsonFile(fs, this.schemaFile, logicSchema);
				
				//write the sql
				HdfsUtil.writeDfsFile(getFs(),this.outputSqlFile, String.join("\n", createTableSqls).getBytes());
				
//				DBUtil.executeSqls(createTableSqls, super.getPc());
				
				logger.info("Schema Refreshed");
			}else{
				logger.info("Schema NoChange");
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return logInfo;
	}
	
	private String buildTransposeMapping(){		
		StringBuilder sb=new StringBuilder();
		for(String tn:logicSchema.getAttrNameMap().keySet()){
			List<String> attrNameList=logicSchema.getAttrNames(tn);
			sb.append("tablename=").append(tn.substring(tableNamePrefix.length())).append("\nfields=");
			boolean mapping=false;
			for(int i=3;i<attrNameList.size();i++){
				String attrName=attrNameList.get(i);				
				sb.append(attrName.split("_")[1]).append("\\,");
				mapping=true;
			}
			if(mapping==true){
				sb.setLength(sb.length()-2);
			}
			sb.append("\n");
		}
		
		return sb.toString();
	}

}
