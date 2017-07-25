package etl.cmd.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import etl.input.StandardXmlInputFormat;
import scala.Tuple2;

public class TestXml2CsvCmd extends TestETLCmd{

	private static final String cmdClassName = "etl.cmd.dynschema.Xml2CsvCmd";
	
	@Test
	public void runGenCsvCmd() throws Exception {
		//
		String inputFolder = "/etltest/pde2/input/";
		String outputFolder = "/etltest/pde2/output/";
		String cfgFolder = "/etltest/pde2/cfg/";
		String dataFolder= "/etltest/pde2/data/";
		
		String csvtransProp = "xml2csv.properties";
		String schemaFileName = "schema";
		
		String[] inputFiles = new String[]{"NODE4g911-oam-blade-3.20170226235246Z2.xml"};
//		String[] inputFiles = new String[]{"NODE4g911-oam-blade-3.seq"};
//		String[] inputFiles = new String[]{"20170226235154Z.xml","20170226235209Z.xml","20170226235246Z.xml","20170226235311Z.xml"};
		
		getFs().delete(new Path(dataFolder), true);
		getFs().mkdirs(new Path(dataFolder));
		
		getFs().delete(new Path(outputFolder), true);
		getFs().mkdirs(new Path(outputFolder));
		
		getFs().delete(new Path(cfgFolder), true);
		getFs().mkdirs(new Path(cfgFolder));
		//schema
		//getFs().delete(new Path("/attmexico/huawei/schema"), true);
		//getFs().copyFromLocalFile(false, true, new Path("./src/main/resources/huawei/schema/"), new Path("/attmexico/huawei/schema"));
		getFs().copyFromLocalFile(new Path(getLocalFolder() + schemaFileName), new Path(cfgFolder + schemaFileName));
		
		//run cmd
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(inputFolder, inputFiles));
		getConf().set("xmlinput.start", "<Trans>");
		getConf().set("xmlinput.end", "</Trans>");

		super.mrTest(rfifs, outputFolder, csvtransProp, cmdClassName, StandardXmlInputFormat.class);
//		super.mrTest(rfifs, outputFolder, csvtransProp, cmdClassName, SequenceFileInputFormat.class);
	}
	
	@Override
	public String getResourceSubFolder() {
		return "xml2csv/";
	}
}
