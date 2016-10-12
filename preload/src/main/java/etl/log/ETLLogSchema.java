package etl.log;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.FieldType;
import etl.util.JsonUtil;
import etl.util.SchemaUtils;

public class ETLLogSchema {
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
		JsonUtil.toLocalJsonFile("logschema.txt", ls);
		LogicSchema ls2 = (LogicSchema) JsonUtil.fromLocalJsonFile("logschema.txt", LogicSchema.class);
		assertTrue(ls2.equals(ls));
	}
	
	@Test
	public void genVerticaSqlFromSchema(){
		SchemaUtils.genCreateSqls("src/main/resources/logschema.txt", "sql/vertica/etllog.sql", "etllog", DBType.VERTICA);
	}
	
	@Test
	public void genHiveSqlFromSchema(){
		SchemaUtils.genCreateSqls("src/main/resources/logschema.txt", "sql/hive/etllog.sql", "etllog", DBType.HIVE);
	}

}
