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
import etl.util.AggregationType;
import etl.util.FieldType;
import etl.util.VarType;

public class CSVSchemaGenerator implements SchemaGenerator {
	public static final Logger logger = LogManager.getLogger(CSVSchemaGenerator.class);
	private static final String DEFAULT_NAME_PREFIX = "UNKNOWN_";

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
				String fieldID;
				String fieldName;
				String fieldType;
				AggregationType fieldAggrType;
				String t;
				String[] tmp;
				String[] tmpID;
				int fieldSize;
				List<String> attributes;
				List<FieldType> attrTypes;

				int k, n;
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
								if (f.getIdIndex() != -1 && f.getIdIndex() < record.size())
									fieldID = record.get(f.getIdIndex());
								else
									fieldID = null;
								
								if (f.getIndex() != -1)
									fieldName = record.get(f.getIndex());
								else
									fieldName = fieldID;
								
								if (fieldName != null)
									fieldName = fieldName.trim();
								
								if (fieldID != null)
									fieldID = fieldID.trim();
								
								if (fieldName != null && fieldName.length() > 0) {
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
									
									if (f.getFieldAggrTypeIndex() != -1)
										fieldAggrType = toAggregationType(record.get(f.getFieldAggrTypeIndex()), config.getFieldAggrTypeMapping());
									else
										fieldAggrType = AggregationType.NONE;
									
									/* Field type mapping if exists */
									if (config.getFieldTypeMapping() != null) {
										t = config.getFieldTypeMapping().get(fieldType);
										if (t != null)
											fieldType = t;
									}
									
									if (f.isMultiple()) {
										tmp = fieldName.split(f.getMultipleSeparator());
										if (fieldID != null && fieldID.length() > 0)
											tmpID = fieldID.split(f.getMultipleSeparator());
										else
											tmpID = null;
										k = 0;
										
										if (f.getMultipleFixedCount() != 0)
											n = f.getMultipleFixedCount();
										else
											n = tmp.length;
										
										while (k < n) {
											if (k < tmp.length)
												fieldName = tmp[k].trim().replaceAll("\\W", "_");
											else if (f.getMultipleDefaultNamePrefix() != null && f.getMultipleDefaultNamePrefix().length() > 0)
												fieldName = f.getMultipleDefaultNamePrefix() + k;
											else
												fieldName = DEFAULT_NAME_PREFIX + record.getRecordNumber() + "_" + k;
											attributes.add(fieldName);
											
											if (tmpID != null && k < tmpID.length && tmpID[k] != null)
												ls.getAttrIdNameMap().put(tmpID[k].trim(), fieldName);
											
											try {
												if (fieldType != null) {
													if ("int".equals(fieldType))
														attrTypes.add(new FieldType(VarType.NUMERIC, 10, 0, fieldAggrType));
													else
														attrTypes.add(new FieldType(VarType.fromValue(fieldType), fieldSize, fieldAggrType));
												} else {
													attrTypes.add(new FieldType(VarType.fromValue(f.getDefaultFieldType()), fieldAggrType));
												}
											} catch (Exception e) {
												logger.error("Unknown type: {}", fieldType);
												attrTypes.add(new FieldType(VarType.OBJECT, fieldAggrType));
											}
											
											k++;
										}
										
									} else {
										fieldName = fieldName.trim().replaceAll("\\W", "_");
										attributes.add(fieldName);
										
										if (fieldID != null && fieldID.length() > 0)
											ls.getAttrIdNameMap().put(fieldID, fieldName);
										
										try {
											if (fieldType != null) {
												if ("int".equals(fieldType))
													attrTypes.add(new FieldType(VarType.NUMERIC, 10, 0, fieldAggrType));
												else
													attrTypes.add(new FieldType(VarType.fromValue(fieldType), fieldSize, fieldAggrType));
											} else {
												attrTypes.add(new FieldType(VarType.fromValue(f.getDefaultFieldType()), fieldAggrType));
											}
										} catch (Exception e) {
											logger.error("Unknown type: {}", fieldType);
											attrTypes.add(new FieldType(VarType.OBJECT, fieldAggrType));
										}
									}
								} else {
									logger.debug("No field name or ID at index {} or {}", f.getIndex(), f.getIdIndex());
								}
							} else {
								logger.trace("Field index {} is out of records range", f.getIndex());
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

	private AggregationType toAggregationType(String aggrType, Map<String, String> aggrTypeMapping) {
		if (aggrType != null) {
			if (aggrTypeMapping != null && aggrTypeMapping.containsKey(aggrType))
				aggrType = aggrTypeMapping.get(aggrType);
			try {
				return aggrType != null && aggrType.length() > 0 ? AggregationType.valueOf(aggrType) : AggregationType.UNKNOWN;
			} catch (IllegalArgumentException e) {
				logger.warn("Unrecognized aggregation type: {}", aggrType);
				return AggregationType.UNKNOWN;
			}
		} else {
			return AggregationType.NONE;
		}
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
					else if (f.getIdHeaderColumnName() != null && headerRecord.get(j) != null && headerRecord.get(j).equals(f.getIdHeaderColumnName()))
						f.setIdIndex(j);
					else if (f.getFieldTypeHeaderColumnName() != null && headerRecord.get(j) != null && headerRecord.get(j).equals(f.getFieldTypeHeaderColumnName()))
						f.setFieldTypeIndex(j);
					else if (f.getFieldSizeHeaderColumnName() != null && headerRecord.get(j) != null && headerRecord.get(j).equals(f.getFieldSizeHeaderColumnName()))
						f.setFieldSizeIndex(j);
					else if (f.getFieldAggrTypeHeaderColumnName() != null && headerRecord.get(j) != null && headerRecord.get(j).equals(f.getFieldAggrTypeHeaderColumnName()))
						f.setFieldAggrTypeIndex(j);
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
