package etl.tools.test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import etl.engine.LogicSchema;
import etl.tools.GenLogicSchema;
import etl.util.FieldType;
import etl.util.Util;

public class TestGenLogicSchema {
	
	@Test
	public void testGenSMSCSchemaFromDB(){
		PropertiesConfiguration pc = Util.getPropertiesConfig("etlengine.properties");
		boolean ret = GenLogicSchema.genLogicSchemaFromDB(pc, "SMSC", "smsc.schema");
		assertTrue(ret);
		
	}
	
	@Test
	public void testGenSgsiwfSchemaFromDB(){
		PropertiesConfiguration pc = Util.getPropertiesConfig("etlengine.properties");
		boolean ret = GenLogicSchema.genLogicSchemaFromDB(pc, "sgsiwf", "sgsiwf.schema");
		assertTrue(ret);
		
	}
	
	@Test
	public void testGenSchemaFromMem(){
		LogicSchema ls = new LogicSchema();
		String[] attNames = new String[]{"endTime","duration","SubNetwork","ManagedElement","Machine",
				"MyCore","UUID","VS.avePerCoreCpuUsage","VS.peakPerCoreCpuUsage","aveActiveSubsNum"};
		List<String> attNamesList = Arrays.asList(attNames);
		FieldType[] attTypes = new FieldType[]{new FieldType("timestamp"),
				new FieldType("varchar(10)"), new FieldType("varchar(70)"),
				new FieldType("varchar(70)"), new FieldType("varchar(54)"),
				new FieldType("numeric(15,5)"),new FieldType("varchar(72)"),
				new FieldType("numeric(15,5)"),new FieldType("numeric(15,5)"), new FieldType("numeric(15,5)")};
		List<FieldType> attTypesList = Arrays.asList(attTypes);
		String tableName = "MyCore_";
		//String tableName1 = "MyCore1_";
		ls.addAttributes(tableName, attNamesList);
		ls.addAttrTypes(tableName, attTypesList);
		//ls.addAttributes(tableName1, attNamesList);
		//ls.addAttrTypes(tableName1, attTypesList);
		Util.toLocalJsonFile("mycore.schema", ls);	
	}

}
