package etl.tools.test;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import etl.tools.GenLogicSchema;
import etl.util.Util;

public class TestGenLogicSchema {
	
	@Test
	public void testGenSchema(){
		PropertiesConfiguration pc = Util.getPropertiesConfig("etlengine.properties");
		GenLogicSchema.genLogicSchemaFromDB(pc, "SMSC", "testls.out");
		
	}

}
