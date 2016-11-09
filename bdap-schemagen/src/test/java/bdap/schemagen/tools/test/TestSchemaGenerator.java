package bdap.schemagen.tools.test;

import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.schemagen.tools.SchemaGenerator;

public class TestSchemaGenerator {
	public static final Logger logger = LogManager.getLogger(TestSchemaGenerator.class);

	public static void main(String[] args) throws Exception {
		URL csvURL = TestSchemaGenerator.class.getResource("/sFemto_OMRT_20150923.csv");
		logger.info("Input csv file: {}", csvURL);
		SchemaGenerator.main(new String[] {csvURL.getPath(), "sfemto"});
		
		csvURL = TestSchemaGenerator.class.getResource("/20151125_LTE eNB SmallCell_OMRT_VzW_Ver.2.1(VSR2.0).csv");
		logger.info("Input csv file: {}", csvURL);
		SchemaGenerator.main(new String[] {csvURL.getPath().replace("%20", " "), "efemto"});
	}

}
