package bdap.schemagen;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.schemagen.config.Config;
import bdap.schemagen.config.FieldConfig;
import bdap.schemagen.config.ItemConfig;
import bdap.schemagen.spi.SchemaGenerator;
import etl.engine.LogicSchema;
import etl.util.FieldType;
import etl.util.VarType;

public class CSVSchemaGenerator implements SchemaGenerator {
	public static final Logger logger = LogManager.getLogger(CSVSchemaGenerator.class);

	public LogicSchema generate(final Reader reader, Config config) throws Exception {
		LogicSchema ls = new LogicSchema();
		CSVParser parser = null;
		try {
			parser = new CSVParser(reader, CSVFormat.DEFAULT.withTrim());

			Iterator<CSVRecord> i = parser.iterator();

			if (i.hasNext()) {
				CSVRecord headerRecord = i.next();
				
				config = readHeaderRecord(headerRecord, config);
				
				CSVRecord record;
				String currentTableID;
				String tableID;
				String tableName;
				String fieldType;
				String t;
				String[] tmp;
				int fieldSize;
				List<String> attributes;
				List<FieldType> attrTypes;

				int k;
				int tableIdIndex = config.getTableId().getIndex();
				int tableNameIndex;
				if (config.getTableName() != null)
					tableNameIndex = config.getTableName().getIndex();
				else
					tableNameIndex = -1;

				if (tableIdIndex == -1) {
					logger.error("Table id must be configured!");
					throw new IllegalArgumentException("Table id must be configured!");
				}

				if (config.getFields() == null || config.getFields().length == 0) {
					logger.error("Table fields must be configured!");
					throw new IllegalArgumentException("Table fields must be configured!");
				}

				attributes = null;
				attrTypes = null;
				currentTableID = "";

				while (i.hasNext()) {
					record = i.next();
					tableID = record.get(tableIdIndex);
					if (tableID != null && tableID.length() > 0 && !currentTableID.equals(tableID)) {
						/* ADD new table schema */
						attributes = new ArrayList<String>();
						attrTypes = new ArrayList<FieldType>();
						tableName = null;
						if (tableNameIndex != -1) {
							tableName = record.get(tableNameIndex);
							if (tableName != null && tableName.length() > 0) {
								tableName = tableName.replaceAll("\\W", "_");
							}
						}

						if (tableName != null && tableName.length() > 0) {
							ls.addAttributes(tableName, attributes);
							ls.addAttrTypes(tableName, attrTypes);
							ls.getTableIdNameMap().put(tableID, tableName);
						} else {
							ls.addAttributes(tableID, attributes);
							ls.addAttrTypes(tableID, attrTypes);
							ls.getTableIdNameMap().put(tableID, tableID);
						}
						
						currentTableID = tableID;
					}
					
					if (attributes != null && attrTypes != null) {
						for (FieldConfig f: config.getFields()) {
							if (f.getIndex() < record.size()) {
								if (f.getIndex() != -1 && record.get(f.getIndex()).trim().length() > 0) {
									if (f.isMultiple()) {
										tmp = record.get(f.getIndex()).trim().split(f.getMultipleSeparator());
										k = 0;
										while (k < tmp.length) {
											attributes.add(tmp[k].trim().replaceAll("\\W", "_"));
											attrTypes.add(new FieldType(VarType.fromValue(f.getDefaultFieldType())));
											k++;
										}
										
									} else {
										attributes.add(record.get(f.getIndex()).trim().replaceAll("\\W", "_"));
	
										if (f.getFieldTypeIndex() != -1)
											fieldType = record.get(f.getFieldTypeIndex());
										else
											fieldType = f.getDefaultFieldType();
										
										if (f.getFieldSizeIndex() != -1) {
											try {
												tmp = record.get(f.getFieldSizeIndex()).split(" ");
												fieldSize = Integer.parseInt(tmp[0]);
											} catch (NumberFormatException e) {
												fieldSize = f.getDefaultFieldSize();
											}
										} else {
											fieldSize = f.getDefaultFieldSize();
										}
										
										/* Field type mapping if exists */
										if (config.getFieldTypeMapping() != null) {
											t = config.getFieldTypeMapping().get(fieldType);
											if (t != null)
												fieldType = t;
										}
	
										try {
											if (fieldType != null) {
												if ("int".equals(fieldType))
													attrTypes.add(new FieldType(VarType.NUMERIC, 10, 0));
												else
													attrTypes.add(new FieldType(VarType.fromValue(fieldType), fieldSize));
											} else {
												attrTypes.add(new FieldType(VarType.fromValue(f.getDefaultFieldType())));
											}
										} catch (Exception e) {
											logger.error("Unknown type: {}", fieldType);
											attrTypes.add(new FieldType(VarType.OBJECT));
										}
									}
								}
							} else {
								logger.trace("Field index {} is not out of records range", f.getIndex());
							}
						}
					}

					logger.debug(record);
				}
			}
		} finally {
			if (parser != null)
				parser.close();
		}
		return ls;
	}

	private Config readHeaderRecord(CSVRecord headerRecord, Config config) {
		int j;
		for (j = 0; j < headerRecord.size(); j++) {
			if (config.getTableId() != null && config.getTableId().getHeaderColumnName() != null && headerRecord.get(j) != null && compareItemColumnName(headerRecord.get(j), config.getTableId()))
				config.getTableId().setIndex(j);
			else if (config.getTableName() != null && config.getTableName().getHeaderColumnName() != null && headerRecord.get(j) != null && compareItemColumnName(headerRecord.get(j), config.getTableName()))
				config.getTableName().setIndex(j);
			else if (config.getFields() != null) {
				for (FieldConfig f: config.getFields()) {
					if (f.getHeaderColumnName() != null && headerRecord.get(j) != null && headerRecord.get(j).equals(f.getHeaderColumnName()))
						f.setIndex(j);
					else if (f.getFieldTypeHeaderColumnName() != null && headerRecord.get(j) != null && headerRecord.get(j).equals(f.getFieldTypeHeaderColumnName()))
						f.setFieldTypeIndex(j);
					else if (f.getFieldSizeHeaderColumnName() != null && headerRecord.get(j) != null && headerRecord.get(j).equals(f.getFieldSizeHeaderColumnName()))
						f.setFieldSizeIndex(j);
				}
			}
		}
		return config;
	}

	private boolean compareItemColumnName(String recordColumn, ItemConfig itemConfig) {
		if ("startsWith".equals(itemConfig.getComparisonMethod()))
			return recordColumn.startsWith(itemConfig.getHeaderColumnName());
		else if ("contains".equals(itemConfig.getComparisonMethod()))
			return recordColumn.contains(itemConfig.getHeaderColumnName());
		else
			return recordColumn.equals(itemConfig.getHeaderColumnName());
	}

	public LogicSchema appendSchema(LogicSchema schema, final Reader reader) {
		return null;
	}

	public LogicSchema insertSchema(LogicSchema ls, LogicSchema commonLs) throws Exception {
		List<String> commonAttrNames;
		List<FieldType> commonAttrTypes;
		if (commonLs != null) {
			commonAttrNames = commonLs.getAttrNames("common");
			commonAttrTypes = commonLs.getAttrTypes("common");
		} else {
			commonAttrNames = null;
			commonAttrTypes = null;
		}
		if (ls != null && commonAttrNames != null)
			for (List<String> attrs: ls.getAttrNameMap().values()) {
				attrs.addAll(0, commonAttrNames);
			}
		if (ls != null && commonAttrTypes != null)
			for (List<FieldType> attrTypes: ls.getAttrTypeMap().values()) {
				attrTypes.addAll(0, commonAttrTypes);
			}
		return ls;
	}

	public LogicSchema joinSchema(LogicSchema ls, LogicSchema additionalLs) throws Exception {
		if (additionalLs != null) {
			List<String> attrNames;
			List<FieldType> attrTypes;
			String tableName;
			for (Map.Entry<String, String> entry : additionalLs.getTableIdNameMap().entrySet()) {
				tableName = ls.getTableIdNameMap().get(entry.getKey());
				if (tableName != null) {
					attrNames = additionalLs.getAttrNames(entry.getValue());
					ls.getAttrNames(tableName).addAll(0, attrNames);
					attrTypes = additionalLs.getAttrTypes(entry.getValue());
					ls.getAttrTypes(tableName).addAll(0, attrTypes);
				} else {
					logger.error("Can't find table name in left logic schema for table ID: {}!", entry.getKey());
				}
			}
		}
		return ls;
	}
}
