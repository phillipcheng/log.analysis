package hpe.mtc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import nokia.xml.GranPeriod;
import nokia.xml.MeasCollecFile;
import nokia.xml.MeasInfo;
import nokia.xml.MeasType;
import nokia.xml.MeasValue;
import nokia.xml.R;

public class XmlProcessor {
	public static final Logger logger = Logger.getLogger(XmlProcessor.class);
	
	public static final String schema_name="schemas.txt";
	
	public static final String createtablesql_name="createtables.sql";
	public static final String droptablesql_name="droptables.sql";
	public static final String trunctablesql_name="trunctables.sql";
	public static final String copysql_name="copys.sql";
	
	public static final String SYSTEM_FIELD_ENDTIME="endTime";
	public static final String SYSTEM_FIELD_SUBNETWORK="SubNetwork";
	public static final String SYSTEM_FIELD_MANAGEDELEMENT="ManagedElement";
	
	//used to generate table name
	private List<String> keyWithValue = new ArrayList<String>();
	private List<String> keySkip = new ArrayList<String>();
	private List<String> systemFieldNames = new ArrayList<String>();
	private List<String> systemFieldTypes = new ArrayList<String>();
	
	private String xmlFolder;
	private String csvFolder;
	private String schemaHistoryFolder;
	private String dataHistoryFolder;
	private String prefix;//used as dbschema name
	private String schemaFileName;
	private LogicSchema logicSchema;
	private Map<String, List<String>> schemaAttrNameUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' name (compared with the org schema)
	private Map<String, List<String>> schemaAttrTypeUpdates = new HashMap<String, List<String>>();//store updated/new tables' attribute parts' type (compared with the org schema)
	private Map<String, List<String>> newTableObjNamesAdded = new HashMap<String, List<String>>();//store new tables' obj name
	private Map<String, List<String>> newTableObjTypesAdded = new HashMap<String, List<String>>();//store new tables' obj type
	private Map<String, BufferedWriter> fvWriterMap = new HashMap<String, BufferedWriter>();//store all the data files generated, key by file name
	private Set<String> tablesUsed = new HashSet<String>(); //the tables this batch of data used
	
	public XmlProcessor(String xmlFolder, String csvFolder, String schemaFolder, String schemaHistoryFolder, 
			String dataHistoryFolder, String prefix){
		keyWithValue.add("PoolType");
		
		keySkip.add("Machine");
		keySkip.add("UUID");
		keySkip.add("PoolId");
		keySkip.add("PoolMember");
		
		systemFieldNames.add(SYSTEM_FIELD_ENDTIME);
		systemFieldNames.add("duration");
		systemFieldNames.add(SYSTEM_FIELD_SUBNETWORK);
		systemFieldNames.add(SYSTEM_FIELD_MANAGEDELEMENT);
		
		systemFieldTypes.add("TIMESTAMP not null");
		systemFieldTypes.add("varchar(10)");
		systemFieldTypes.add("varchar(70)");
		systemFieldTypes.add("varchar(70)");
		
		this.xmlFolder = xmlFolder;
		this.csvFolder = csvFolder;
		this.schemaHistoryFolder = schemaHistoryFolder;
		this.dataHistoryFolder = dataHistoryFolder;
		this.prefix = prefix;
		this.schemaFileName = schemaFolder + prefix +"." + schema_name;
		this.logicSchema = new LogicSchema();
		if (new File(schemaFileName).exists()){
			logicSchema = LogicSchema.fromFile(schemaFileName);
		}	
	}
	
	public static MeasCollecFile unmarshal(String inputXml){
		try {
			JAXBContext jc = JAXBContext.newInstance("nokia.xml");
		    Unmarshaller u = jc.createUnmarshaller();
		    return (MeasCollecFile) u.unmarshal(new File(inputXml));
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
	}
	
	public String generateTableName(TreeMap<String, String> moldParams){
		StringBuffer sb = new StringBuffer();
		for (String key: moldParams.keySet()){
			if (!keySkip.contains(key)){
				if (keyWithValue.contains(key)){
					sb.append(String.format("%s_%s", key, moldParams.get(key)));
				}else{
					sb.append(key);
				}
				sb.append("_");
			}
		}
		return sb.toString();
	}
	
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	private void closeCsvWriters(){
		for (BufferedWriter bw:fvWriterMap.values()){
			if (bw!=null){
				try {
					bw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	private String getOutputDataFileName(String tableName, String timeStr){
		return csvFolder + timeStr + "_" + tableName + ".csv";
	}
	private void genData(MeasInfo mi, Map<String, String> localDnMap, List<String> orgSchemaAttributes, List<String> mtcl, String tableName, 
			Map<String, BufferedWriter> fvWriterMap, String startProcessTime) throws Exception {
		GranPeriod gp = mi.getGranPeriod();
		//gen value idx mapping
		Map<Integer,Integer> mapping = new HashMap<Integer, Integer>();//current attribute maps to which schema attribute
		for (int i=0; i<orgSchemaAttributes.size(); i++){
			String attr = orgSchemaAttributes.get(i);
			int idx = mtcl.indexOf(attr);
			if (idx!=-1){
				mapping.put(idx, i);
			}
		}
		for (MeasValue mvl:mi.getMeasValue()){
			List<String> fieldValues = new ArrayList<String>();
			//system values
			fieldValues.add(gp.getEndTime());
			fieldValues.add(gp.getDuration());
			fieldValues.add(localDnMap.get(SYSTEM_FIELD_SUBNETWORK));
			fieldValues.add(localDnMap.get(SYSTEM_FIELD_MANAGEDELEMENT));
			//object values
			TreeMap<String, String> kvs = Util.parseMapParams(mvl.getMeasObjLdn());
			for (String v:kvs.values()){
				fieldValues.add(v);
			}
			String[] vs = new String[orgSchemaAttributes.size()];
			for (int i=0; i<mvl.getR().size(); i++){
				String v = mvl.getR().get(i).getContent();
				vs[mapping.get(i)]=v;
			}
			fieldValues.addAll(Arrays.asList(vs));
			String outputFileName = getOutputDataFileName(tableName, startProcessTime);
			BufferedWriter osw = fvWriterMap.get(outputFileName);
			if (osw ==null) {
				osw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileName)));
				fvWriterMap.put(outputFileName, osw);
			}
			String csv = Util.getCsv(fieldValues);
			osw.write(csv);
		}
	}
	
	private boolean checkSchemaUpdateOrGenData(String[] inputFileNames, String strProcessStartTime){
		boolean schemaUpdated=false;
		try {
			for (String inputFileName: inputFileNames){
				String ifn = xmlFolder + inputFileName;
				MeasCollecFile mf = XmlProcessor.unmarshal(ifn);
				Map<String, String> localDnMap = Util.parseMapParams(mf.getFileHeader().getFileSender().getLocalDn());
				List<MeasInfo> ml = mf.getMeasData().getMeasInfo();
				for (MeasInfo mi:ml){
					MeasValue mv0 = mi.getMeasValue().get(0);
					String moldn = mv0.getMeasObjLdn();
					TreeMap<String, String> moldParams = Util.parseMapParams(moldn);
					String tableName = generateTableName(moldParams);
					tablesUsed.add(tableName);
					List<String> orgSchemaAttributes = null;
					if (logicSchema.hasAttrNames(tableName)){
						orgSchemaAttributes = new ArrayList<String>();
						orgSchemaAttributes.addAll(logicSchema.getAttrNames(tableName));
					}
					{//merge the origin and newUpdates
						List<String> newSchemaAttributes = schemaAttrNameUpdates.get(tableName);
						if (newSchemaAttributes!=null){
							if (orgSchemaAttributes == null)
								orgSchemaAttributes = newSchemaAttributes;
							else{
								orgSchemaAttributes.addAll(newSchemaAttributes);
							}
						}
					}
					if (orgSchemaAttributes!=null){
						List<String> mtcl = new ArrayList<String>();
						for (MeasType mt: mi.getMeasType()){
							mtcl.add(mt.getContent());
						}
						//check new attribute
						List<String> newAttrNames = new ArrayList<String>();
						List<String> newAttrTypes = new ArrayList<String>();
						for (int i=0; i<mtcl.size(); i++){
							String mtc = mtcl.get(i);
							if (!orgSchemaAttributes.contains(mtc)){
								newAttrNames.add(mtc);
								newAttrTypes.add(Util.guessType(mv0.getR().get(i).getContent()));
							}
						}
						if (newAttrNames.size()>0){
							if (schemaAttrNameUpdates.containsKey(tableName)){
								newAttrNames.addAll(0, schemaAttrNameUpdates.get(tableName));
								newAttrTypes.addAll(0, schemaAttrTypeUpdates.get(tableName));
							}
							schemaAttrNameUpdates.put(tableName, newAttrNames);
							schemaAttrTypeUpdates.put(tableName, newAttrTypes);
							schemaUpdated=true;
						}else{
							if (!schemaUpdated){//gen data
								genData(mi, localDnMap, orgSchemaAttributes, mtcl, tableName, fvWriterMap, strProcessStartTime);
							}
						}
					}else{
						//new table needed
						List<String> onlyAttrNamesList = new ArrayList<String>();
						for (MeasType mt: mi.getMeasType()){
							onlyAttrNamesList.add(mt.getContent());
						}
						List<String> onlyAttrTypesList = new ArrayList<String>();
						for (R r: mv0.getR()){
							onlyAttrTypesList.add(Util.guessType(r.getContent()));
						}
						schemaAttrNameUpdates.put(tableName, onlyAttrNamesList);
						schemaAttrTypeUpdates.put(tableName, onlyAttrTypesList);
						//
						List<String> objNameList = new ArrayList<String>();
						List<String> objTypeList = new ArrayList<String>();
						for (String key: moldParams.keySet()){
							objNameList.add(key);
							objTypeList.add(Util.guessType(moldParams.get(key)));
						}
						newTableObjNamesAdded.put(tableName, objNameList);
						newTableObjTypesAdded.put(tableName, objTypeList);
						schemaUpdated=true;
					}
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}finally{
			closeCsvWriters();
		}
		return schemaUpdated;
	}
	
	public void process(){
		String strProcessStartTime = sdf.format(new Date());
		File xmlFolderFile = new File(xmlFolder);
		String[] inputFileNames = xmlFolderFile.list();
		try {
			boolean schemaUpdated = checkSchemaUpdateOrGenData(inputFileNames, strProcessStartTime);
			//generate schema files
			if (schemaUpdated){
				List<String> createTableSqls = new ArrayList<String>();
				List<String> dropTableSqls = new ArrayList<String>();
				List<String> truncTableSqls = new ArrayList<String>();
				List<String> copysqls = new ArrayList<String>();
				for(String tn:schemaAttrNameUpdates.keySet()){
					List<String> fieldNameList = new ArrayList<String>(); 
					List<String> fieldTypeList = new ArrayList<String>();
					if (logicSchema.getAttrNames(tn)!=null){//update table
						//gen update sql
						fieldNameList.addAll(schemaAttrNameUpdates.get(tn));
						fieldTypeList.addAll(schemaAttrTypeUpdates.get(tn));
						createTableSqls.addAll(Util.genUpdateTableSql(fieldNameList, fieldTypeList, tn, prefix));
						logicSchema.addAttributes(tn, schemaAttrNameUpdates.get(tn));
					}else{//new table
						//update to logic schema
						logicSchema.addAttributes(tn, schemaAttrNameUpdates.get(tn));
						logicSchema.addObjParams(tn, newTableObjNamesAdded.get(tn));
						//gen create sql
						fieldNameList.addAll(systemFieldNames);
						fieldTypeList.addAll(systemFieldTypes);
						fieldNameList.addAll(newTableObjNamesAdded.get(tn));
						fieldTypeList.addAll(newTableObjTypesAdded.get(tn));
						fieldNameList.addAll(schemaAttrNameUpdates.get(tn));
						fieldTypeList.addAll(schemaAttrTypeUpdates.get(tn));
						createTableSqls.add(Util.genCreateTableSql(fieldNameList, fieldTypeList, tn, prefix));
						dropTableSqls.add(Util.genDropTableSql(tn, prefix));
						truncTableSqls.add(Util.genTruncTableSql(tn, prefix));
					}
					//gen copys.sql for reference
					fieldNameList.clear();
					fieldNameList.addAll(systemFieldNames);
					fieldNameList.addAll(logicSchema.getObjParams(tn));
					fieldNameList.addAll(logicSchema.getAttrNames(tn));
					String copySql = Util.genCopySql(fieldNameList, tn, csvFolder, prefix);//having %s as the placeholder for real file name
					copysqls.add(copySql);
				}
				//execute the schemas
				Util.executeSqls(createTableSqls);
				
				//gen schema files to history dir for reference
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, createtablesql_name, strProcessStartTime), createTableSqls);
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, droptablesql_name, strProcessStartTime), dropTableSqls);
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, trunctablesql_name, strProcessStartTime), truncTableSqls);
				Util.writeFile(String.format("%s%s.%s_%s", schemaHistoryFolder, prefix, copysql_name, strProcessStartTime), copysqls);
				
				//gen logic schema
				LogicSchema.toFile(schemaFileName, logicSchema);
				
				//gen data again using new schema
				fvWriterMap.clear();
				schemaAttrNameUpdates.clear();
				checkSchemaUpdateOrGenData(inputFileNames, strProcessStartTime);
				
				//mv the raw-xml files to lake
				//TODO
			}
			//copy the data to db
			List<String> copysqls = new ArrayList<String>();
			for (String tn: tablesUsed){
				List<String> fieldNameList = new ArrayList<String>();
				fieldNameList.addAll(systemFieldNames);
				fieldNameList.addAll(logicSchema.getObjParams(tn));
				fieldNameList.addAll(logicSchema.getAttrNames(tn));
				String copySql = Util.genCopySql(fieldNameList, tn, csvFolder, prefix);//having %s as the placeholder for real file name
				copySql = copySql.replace("%s", getOutputDataFileName(tn, strProcessStartTime));
				copysqls.add(copySql);
			}
			Util.executeSqls(copysqls);
			
			//mv the csv files to lake
			//TODO
		}catch(Exception e){
			logger.error("", e);
		}
	}
}
