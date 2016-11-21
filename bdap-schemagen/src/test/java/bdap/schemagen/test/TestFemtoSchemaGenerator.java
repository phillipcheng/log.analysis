package bdap.schemagen.test;

import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.schemagen.femto.SchemaGenerator;

public class TestFemtoSchemaGenerator {
	public static final Logger logger = LogManager.getLogger(TestFemtoSchemaGenerator.class);

	private static final String resFolder="/femto/";
	
	@Test
	public void testFemto() throws Exception {
		URL csvURL = TestFemtoSchemaGenerator.class.getResource(resFolder + "sFemto_OMRT_20150923.csv");
		logger.info("Input csv file: {}", csvURL);
		SchemaGenerator.main(new String[] {csvURL.getPath(), "sfemto"});
		
		csvURL = TestFemtoSchemaGenerator.class.getResource(resFolder + "20151125_LTE eNB SmallCell_OMRT_VzW_Ver.2.1(VSR2.0).csv");
		logger.info("Input csv file: {}", csvURL);
		SchemaGenerator.main(new String[] {csvURL.getPath().replace("%20", " "), "efemto"});
	}

}
