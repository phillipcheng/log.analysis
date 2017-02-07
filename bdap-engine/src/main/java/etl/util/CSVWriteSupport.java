package etl.util;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import bdap.util.EngineConf;
import bdap.util.PropertiesUtil;
import etl.cmd.SchemaETLCmd;
import etl.engine.ETLCmd;
import etl.engine.LogicSchema;

public class CSVWriteSupport extends WriteSupport<Text> {
	public static final Logger logger = LogManager.getLogger(CSVWriteSupport.class);
	private static final String DEFAULT_FS = "fs.defaultFS";
	private static final byte[] EMPTY_INT96 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	private RecordConsumer recordConsumer;
	private MessageType rootSchema;
	private LogicSchema rootLogicSchema;
	private String tableName;

	public WriteSupport.WriteContext init(Configuration configuration) {
		Map<String, String> extraMetaData = new HashMap<String, String>();
		// extraMetaData.put(AvroReadSupport.AVRO_SCHEMA_METADATA_KEY, rootAvroSchema.toString());
		rootLogicSchema = parse(configuration.get(DEFAULT_FS), configuration.get(SchemaETLCmd.cfgkey_schema_file));
		rootSchema = SchemaUtils.convertToParquetSchema(rootLogicSchema, tableName);
		return new WriteSupport.WriteContext(rootSchema, extraMetaData);
	}

	public void initTableName(Configuration configuration, Path file) {
		Map<String, Object> sysVars = new HashMap<String, Object>();
		String pathName = file.toString();
		sysVars.put(ETLCmd.VAR_NAME_PATH_NAME, pathName);
		String fileName = file.getName();
		sysVars.put(ETLCmd.VAR_NAME_FILE_NAME, fileName);
		
		String strFileTableMap = configuration.get(ETLCmd.cfgkey_file_table_map);
		if (strFileTableMap == null) {
			String staticConf = configuration.get(EngineConf.cfgkey_staticconfigfile);
			if (staticConf!=null){
				PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(staticConf);
				strFileTableMap = pc.getString(ETLCmd.cfgkey_file_table_map);
			}
		}
		CompiledScript expFileTableMap = null;
		if (strFileTableMap!=null){
			expFileTableMap = ScriptEngineUtil.compileScript(strFileTableMap);
			logger.info(String.format("fileTableMapExp:%s", expFileTableMap));
		}
		
		if (expFileTableMap!=null){
			tableName = ScriptEngineUtil.eval(expFileTableMap, sysVars);
		}else{
			tableName = pathName;
		}
		
		logger.info("table name: {}", tableName);
	}

	private LogicSchema parse(String defaultFs, String location) {
		if (location != null && location.length() > 0) {
			if (SchemaUtils.existsRemoteJsonPath(defaultFs, location)) {
				return SchemaUtils.fromRemoteJsonPath(defaultFs, location, LogicSchema.class);
			} else {
				logger.warn(String.format("schema file %s not exists.", location));
				return SchemaUtils.newRemoteInstance(defaultFs, location);
			}
		} else
			return null;
	}
	
	public void prepareForWrite(RecordConsumer recordConsumer) {
		this.recordConsumer = recordConsumer;
	}
	
	private Binary decimalToBinary(final HiveDecimal hiveDecimal, int prec, int scale) {
		byte[] decimalBytes = hiveDecimal.setScale(scale).unscaledValue().toByteArray();
		// Estimated number of bytes needed.
		int precToBytes = SchemaUtils.minBytes(prec);
		if (precToBytes == decimalBytes.length) {
			// No padding needed.
			return Binary.fromConstantByteArray(decimalBytes);
		}
		byte[] tgt = new byte[precToBytes];
		if (hiveDecimal.signum() == -1) {
			// For negative number, initializing bits to 1
			for (int i = 0; i < precToBytes; i++) {
				tgt[i] |= 0xFF;
			}
		}
		System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding
																										// leading
																										// zeroes/ones.
		return Binary.fromConstantByteArray(tgt);
	}
	
	public void write(Text row) {
		CSVParser parser = null;
		try {
			int index;
			parser = CSVParser.parse(row.toString(), CSVFormat.DEFAULT.withTrim());
		    
			for (CSVRecord csv : parser.getRecords()) {
				recordConsumer.startMessage();
				Type fieldType;
				String fieldName;
				HiveDecimal vDecimal;
				DecimalMetadata dm;
				PrimitiveType pt;
				Date t;
				index = 0;
				for (String fieldValue: csv) {
					// rootLogicSchema.get
					fieldType = rootSchema.getType(index);
					fieldName = fieldType.getName();
			        recordConsumer.startField(fieldName, index);
					if (PrimitiveTypeName.INT32.equals(fieldType.asPrimitiveType().getPrimitiveTypeName())) {
						pt = fieldType.asPrimitiveType();
						if (OriginalType.DATE.equals(pt.getOriginalType())) {
							try {
								t = FieldType.sdateFormat.parse(fieldValue);
								if (t != null)
									recordConsumer.addInteger(DateUtil.dateToDays(t));
								else {
									logger.error("{} Parse to timestamp failed", fieldValue);
									recordConsumer.addInteger(0);
								}
							} catch (ParseException e) {
								logger.error(e.getMessage(), e);
								recordConsumer.addLong(0);
							}
						} else
							recordConsumer.addInteger(Integer.parseInt(fieldValue));
					} else if (PrimitiveTypeName.INT64.equals(fieldType.asPrimitiveType().getPrimitiveTypeName())) {
						recordConsumer.addLong(Long.parseLong(fieldValue));
					} else if (PrimitiveTypeName.INT96.equals(fieldType.asPrimitiveType().getPrimitiveTypeName())) {
						pt = fieldType.asPrimitiveType();
						if (pt.getOriginalType() == null) { /* Default is timestamp */
							try {
								t = FieldType.sdatetimeFormat.parse(fieldValue);
								if (t != null)
									recordConsumer.addBinary(NanoTimeUtils.getNanoTime(t.getTime(), false).toBinary());
								else {
									logger.error("{} Parse to timestamp failed", fieldValue);
									recordConsumer.addBinary(Binary.fromConstantByteArray(EMPTY_INT96));
								}
							} catch (ParseException e) {
								logger.error(e.getMessage(), e);
								recordConsumer.addBinary(Binary.fromConstantByteArray(EMPTY_INT96));
							}
						} else {
							logger.error("Unsupported parquet field type: {} for primitive type INT96", pt.getOriginalType());
						}
					} else if (PrimitiveTypeName.FLOAT.equals(fieldType.asPrimitiveType().getPrimitiveTypeName()))
						recordConsumer.addFloat(Float.parseFloat(fieldValue));
					else if (PrimitiveTypeName.DOUBLE.equals(fieldType.asPrimitiveType().getPrimitiveTypeName()))
						recordConsumer.addDouble(Double.parseDouble(fieldValue));
					else if (PrimitiveTypeName.BOOLEAN.equals(fieldType.asPrimitiveType().getPrimitiveTypeName()))
						recordConsumer.addBoolean(Boolean.parseBoolean(fieldValue));
					else if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY.equals(fieldType.asPrimitiveType().getPrimitiveTypeName())) {
						pt = fieldType.asPrimitiveType();
						if (OriginalType.DECIMAL.equals(pt.getOriginalType())) {
							vDecimal = HiveDecimal.create(fieldValue);
							dm = pt.getDecimalMetadata();
							if (vDecimal != null)
								recordConsumer.addBinary(decimalToBinary(vDecimal, dm.getPrecision(), dm.getScale()));
							else
								recordConsumer.addBinary(decimalToBinary(HiveDecimal.ZERO, dm.getPrecision(), dm.getScale()));
						} else {
							recordConsumer.addBinary(Binary.fromString(fieldValue));
						}
					} else if (PrimitiveTypeName.BINARY.equals(fieldType.asPrimitiveType().getPrimitiveTypeName())) {
						recordConsumer.addBinary(Binary.fromString(fieldValue));
					} else {
						logger.error("Unsupported parquet field type: {}", fieldType);
					}
			        recordConsumer.endField(fieldName, index);
					index ++;
				}	
			    recordConsumer.endMessage();
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			
		} finally {
			if (parser != null)
				try {
					parser.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
	}
}
