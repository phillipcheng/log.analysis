package etl.cmd.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.junit.Test;

import etl.cmd.test.TestETLCmd;
import etl.input.XmlInputFormat;
import scala.Tuple2;

public class TestCustomCsvSplitCmd extends TestETLCmd{

	private static final String cmdClassName = "etl.cmd.CustomCsvSplitCmd";
	
	@Test
	public void runGenCsvCmd() throws Exception {
		//
		String staticCfgName = "fgwemsom.properties";
		String localSchemaFileName = "fgwemsom.txt";
		String inputFolder = "/etltest/sftp/";
		String outputFolder = "/etltest/sftp/output/";
		String cfgFolder = "/etltest/sftp/cfg/";
		String dataFolder= "/etltest/sftp/data/";
		
		String schemaFolder="/etltest/common/schema/";
		
		String csvtransProp = this.getResourceSubFolder() + staticCfgName;
		
//		String[] inputFiles = new String[]{"NODE4g911-oam-blade-5.20170226235154Z.20170226235654Z.xml"};
		String[] inputFiles = new String[]{"sequence6e9b33ce-c910-4919-a966-d3c3a1a168a4.seq"};
//		String[] inputFiles = new String[]{"NODE4g911-oam-blade-3.seq"};
//		String[] inputFiles = new String[]{"20170226235154Z.xml","20170226235209Z.xml","20170226235246Z.xml","20170226235311Z.xml"};
		
//		getFs().delete(new Path(dataFolder), true);
//		getFs().mkdirs(new Path(dataFolder));
//		
//		getFs().delete(new Path(outputFolder), true);
//		getFs().mkdirs(new Path(outputFolder));
//		
//		getFs().delete(new Path(cfgFolder), true);
//		getFs().mkdirs(new Path(cfgFolder));
		//schema
		//getFs().delete(new Path("/attmexico/huawei/schema"), true);
		//copy schema file
		System.out.println(getLocalFolder());
		getFs().copyFromLocalFile(new Path(getLocalFolder() + localSchemaFileName), new Path(schemaFolder + localSchemaFileName));
				
		
		//run cmd
		List<Tuple2<String, String[]>> rfifs = new ArrayList<Tuple2<String, String[]>>();
		rfifs.add(new Tuple2<String, String[]>(inputFolder, inputFiles));
//		getConf().set("xmlinput.start", "<MALSTransLog ");
//		getConf().set("xmlinput.end", "</MALSTransLog>");
//		getConf().set("xmlinput.row.start", "<Trans>");
//		getConf().set("xmlinput.row.end", "</Trans>");
//		getConf().set("xmlinput.row.max.number", "1000");

//		super.mrTest(rfifs, outputFolder, csvtransProp, cmdClassName, XmlInputFormat.class);
		super.mrTest(rfifs, outputFolder, staticCfgName, cmdClassName, SequenceFileInputFormat.class);
	}
	
	@Override
	public String getResourceSubFolder() {
		return "customsplitcsv/";
	}
}
