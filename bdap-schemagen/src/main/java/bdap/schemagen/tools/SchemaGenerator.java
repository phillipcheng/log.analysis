package bdap.schemagen.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.FieldType;
import etl.util.SchemaUtils;
import etl.util.VarType;

public class SchemaGenerator {
	public static final Logger logger = LogManager.getLogger(SchemaGenerator.class);
	
	private static Object fromJsonString(String json, Class<LogicSchema> clazz){
		ObjectMapper mapper = new ObjectMapper();
		//mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		try {
			Object t = mapper.readValue(json, clazz);
			return t;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	private static String toJsonString(Object ls){
		ObjectMapper om = new ObjectMapper();
		//om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		ObjectWriter ow = om.writer().withDefaultPrettyPrinter();
		try {
			String json = ow.writeValueAsString(ls);
			return json;
		} catch (JsonProcessingException e) {
			logger.error("",e );
			return null;
		}
	}
	
	private static void toLocalJsonFile(String file, Object ls){
		PrintWriter out = null;
		try{
			out = new PrintWriter(file, "utf8");
			out.println(toJsonString(ls));
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (out!=null)
				out.close();
		}
	}

	public static void main(String[] args) throws Exception {
		CSVParser parser = null;
		String dbSchema;
		
		if (args.length > 1)
			dbSchema = args[1];
		else
			dbSchema = "mydb";
		
		if (args.length > 0) {
			try {
				LogicSchema ls = new LogicSchema();
				File file = new File(args[0]);
				String fileName = file.getName().substring(0, file.getName().lastIndexOf('.'));
				parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT.withTrim());
				
				InputStream in = SchemaGenerator.class.getResourceAsStream("/schema-generator-cfg.json");
				BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf8"));
				StringBuilder strbuf = new StringBuilder();
				String str;
				while ((str = reader.readLine()) != null) {
					strbuf.append(str);
				}
				reader.close();
				in.close();
				
				LogicSchema commonLs = (LogicSchema) fromJsonString(strbuf.toString(), LogicSchema.class);
				List<String> commonAttrNames;
				List<FieldType> commonAttrTypes;
				if (commonLs != null) {
					commonAttrNames = commonLs.getAttrNames("common");
					commonAttrTypes = commonLs.getAttrTypes("common");
				} else {
					commonAttrNames = null;
					commonAttrTypes = null;
				}
				
				Iterator<CSVRecord> i = parser.iterator();
				
				if (i.hasNext()) {
					CSVRecord headerRecord = i.next();
					CSVRecord record;
					String familyID;
					String familyName;
					String registerType;
					String[] tmp;
					int registerSize;
					List<String> attributes;
					List<FieldType> attrTypes;
					Map<String, String> fileTableMapping;
					
					int j, k, n;
					int familyIDIndex = -1;
					int familyNameIndex = -1;
					int omRegisterIndex = -1;
					int omRegisterTypeIndex = -1;
					int omRegisterSizeIndex = -1;
					int omIndexesIndex = -1;
					
					for (j = 0; j < headerRecord.size(); j ++) {
						if (headerRecord.get(j) != null && headerRecord.get(j).startsWith("OM Keys"))
							familyIDIndex = j;
						else if (headerRecord.get(j) != null && headerRecord.get(j).startsWith("OM Groups"))
							familyNameIndex = j;
						else if (headerRecord.get(j) != null && headerRecord.get(j).startsWith("OM Registers"))
							omRegisterIndex = j;
						else if (headerRecord.get(j) != null && headerRecord.get(j).startsWith("OM Types"))
							omRegisterTypeIndex = j;
						else if (headerRecord.get(j) != null && headerRecord.get(j).startsWith("OM Size"))
							omRegisterSizeIndex = j;
						else if (headerRecord.get(j) != null && headerRecord.get(j).startsWith("OM Indexes"))
							omIndexesIndex = j;
					}
					
					if (familyIDIndex == -1) {
						logger.error("OM Keys must presend in header!");
						return;
					}
					
					if (omRegisterIndex == -1) {
						logger.error("OM Registers must present in header!");
						return;
					}
					
					if (omRegisterTypeIndex == -1) {
						logger.error("OM Types must present in header!");
					}
					
					attributes = null;
					attrTypes = null;
					fileTableMapping = new HashMap<String, String>();
					
					while(i.hasNext()) {
						record = i.next();
						familyID = record.get(familyIDIndex);
						if (familyID != null && familyID.length() > 0) {
							/* ADD new table schema */
							attributes = new ArrayList<String>();
							attrTypes = new ArrayList<FieldType>();
							familyName = null;
							if (familyNameIndex != -1) {
								familyName = record.get(familyNameIndex);
								if (familyName != null && familyName.length() > 0) {
									familyName = familyName.replaceAll("\\W", "_"); 
								}
							}
							
							if (commonAttrNames != null) {
								attributes.addAll(commonAttrNames);
								if (omIndexesIndex != -1 && record.get(omIndexesIndex).trim().length() > 0) {
									tmp = record.get(omIndexesIndex).trim().split(",");
									k = 0;
									n = attributes.size();
									while (k < tmp.length) {
										attributes.set(n - 6 + k, tmp[k].trim());
										k ++;
									}
								}
							}
							
							if (commonAttrTypes != null)
								attrTypes.addAll(commonAttrTypes);
							
							if (familyName != null && familyName.length() > 0) {
								ls.addAttributes(familyName, attributes);
								ls.addAttrTypes(familyName, attrTypes);
								fileTableMapping.put(familyID, familyName);
							} else {
								ls.addAttributes(familyID, attributes);
								ls.addAttrTypes(familyID, attrTypes);
								fileTableMapping.put(familyID, familyID);
							}
							
						} else if (attributes != null && attrTypes != null) {						
							attributes.add(record.get(omRegisterIndex));
							registerType = record.get(omRegisterTypeIndex);
							try {
								tmp = record.get(omRegisterSizeIndex).split(" ");
								registerSize = Integer.parseInt(tmp[0]);
							} catch (NumberFormatException e) {
								registerSize = -1;
							};
							
							try {
								if ("int".equals(registerType))
									attrTypes.add(new FieldType(VarType.NUMERIC, 10, 0));
								else
									attrTypes.add(new FieldType(VarType.fromValue(registerType), registerSize));
							} catch (Exception e) {
								logger.error("Unknown type: {}", registerType);
								attrTypes.add(new FieldType(VarType.OBJECT));
							}
						}
						
						logger.debug(record);
					}
					
					toLocalJsonFile(fileName + ".ls", ls);
					
					List<String> ddl = SchemaUtils.genCreateSqlByLogicSchema(ls, dbSchema, DBType.VERTICA);
					if (ddl != null && ddl.size() > 0) {
						BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + ".ddl"));
						for (String line: ddl) {
							writer.write(line);
							writer.write(';');
							writer.newLine();
						}
			            writer.close();
					}
	
					if (fileTableMapping.size() > 0) {
						BufferedWriter writer = new BufferedWriter(new FileWriter(fileName + "file_table_mapping.properties"));
						for (Map.Entry<String, String> e : fileTableMapping.entrySet()) {
							writer.write("filename="+e.getKey());
							writer.newLine();
							writer.write("tablename="+e.getValue());
							writer.newLine();
						}
						writer.close();
					}
					
				} else {
					logger.error("No records in the CSV file!");
				}
				
				
			} finally {
				if (parser != null)
					parser.close();
			}
			
		} else {
			System.out.println("Usage: java -jar schema-generator-r0.2.0.jar <path of schema definition csv file> [db schema]");
		}
	}

}
