package etl.log.test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import bdap.util.JsonUtil;
import etl.engine.LogicSchema;
import etl.engine.types.DBType;
import etl.util.FieldType;
import etl.util.SchemaUtils;
import etl.util.StoreFormat;

public class TestETLLogSchema {
	@Test
	public void genETLLogSchema(){
		LogicSchema ls = new LogicSchema();
		
		{
			String[] attNames = new String[]{"startdt","enddt",
					"wfname","wfid","actionname",
					"cnt1","cnt2","cnt3","cnt4"};
			FieldType[] attTypes = new FieldType[]{new FieldType("timestamp"),new FieldType("timestamp"),
					new FieldType("varchar(100)"), new FieldType("varchar(200)"),new FieldType("varchar(100)"),
					new FieldType("numeric(20,5)"), new FieldType("numeric(20,5)"),new FieldType("numeric(20,5)"), new FieldType("numeric(20,5)")};
			List<String> attNamesList = Arrays.asList(attNames);
			List<FieldType> attTypesList = Arrays.asList(attTypes);
			String tableName = "etlstat";
			ls.addAttributes(tableName, attNamesList);
			ls.addAttrTypes(tableName, attTypesList);
		}
		{
			String[] attNames = new String[]{"startdt","wfname","wfid","actionname","exception"};
			FieldType[] attTypes = new FieldType[]{new FieldType("timestamp"),
					new FieldType("varchar(100)"), new FieldType("varchar(200)"),new FieldType("varchar(100)"),new FieldType("varchar(5000)")};
			List<String> attNamesList = Arrays.asList(attNames);
			List<FieldType> attTypesList = Arrays.asList(attTypes);
			String tableName = "etlexception";
			ls.addAttributes(tableName, attNamesList);
			ls.addAttrTypes(tableName, attTypesList);
			
		}
		JsonUtil.toLocalJsonFile(getResourceFolder() + "generated/logschema.txt", ls);
		LogicSchema ls2 = SchemaUtils.fromLocalJsonPath(getResourceFolder() + "generated/logschema.txt", LogicSchema.class);
		assertTrue(ls2.equals(ls));
	}
	
	@Test
	public void genVerticaSqlFromSchema() throws Exception{
		SchemaUtils.genCreateSqls(getResourceFolder() + "logschema.txt", getResourceFolder() + "generated/etllog.vertica.sql", "etllog", 
				DBType.VERTICA, StoreFormat.text);
		File file1 = new File(getResourceFolder() + "etllog.vertica.sql");
		File file2 = new File(getResourceFolder() + "generated/etllog.vertica.sql");
		assertTrue(FileUtils.contentEquals(file1, file2));
	}
	
	@Test
	public void genHiveSqlFromSchema() throws Exception{
		SchemaUtils.genCreateSqls(getResourceFolder() + "logschema.txt", getResourceFolder() + "generated/etllog.hive.sql", "etllog", 
				DBType.HIVE, StoreFormat.text);
		File file1 = new File(getResourceFolder() + "etllog.hive.sql");
		File file2 = new File(getResourceFolder() + "generated/etllog.hive.sql");
		assertTrue(FileUtils.contentEquals(file1, file2));
	}
	
	public String getResourceFolder(){
		return "src/test/resources/etllog/";
	}

}
