package etl.mapred.test;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.junit.Test;

import bdap.util.HdfsUtil;
import etl.cmd.test.TestETLCmd;
import scala.Tuple2;

public class TestMapred extends TestETLCmd{
	private static final long serialVersionUID = 1L;

	@Override
	public String getResourceSubFolder() {
		return "xmltocsv/";
	}
	
	public List<String> oldMapTest(String remoteInputFolder, String remoteOutputFolder, String[] inputDataFiles, 
			Class inputFormatClass) throws Exception {
		try {
			getFs().delete(new Path(remoteInputFolder), true);
			getFs().delete(new Path(remoteOutputFolder), true);
			getFs().mkdirs(new Path(remoteInputFolder));
			for (String csvFile : inputDataFiles) {
				getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile), new Path(remoteInputFolder + csvFile));
			}
			// run job
			JobConf job = new JobConf(getConf());
			job.setMapperClass(OldMapper.class);
			job.setNumReduceTasks(0);// no reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setInputFormat(inputFormatClass);
			FileInputFormat.addInputPath(job, new Path(remoteInputFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteOutputFolder));
			JobClient jc = new JobClient(getConf());
			RunningJob rj = jc.submitJob(job);
			while (!rj.isComplete()){
				Thread.sleep(1000);
			}
			// assertion
			List<String> output = HdfsUtil.stringsFromDfsFolder(getFs(), remoteOutputFolder);
			return output;
		} catch (Exception e) {
			logger.error("", e);
		}
		return null;
	}
	
	@Test
	public void test1() throws Exception{
		String inputFolder = "/test/xml2csv/input/";
		String outputFolder = "/test/xml2csv/output/";
		String schemaFolder="/test/xml2csv/schema/";
		
		String[] inputFiles = new String[]{"dynschema_test1_data.xml"};
		String localSchemaFile = "dynschema_test1_schemas.txt";
		String remoteSchemaFile = "schemas.txt";
		
		//schema
		getFs().copyFromLocalFile(false, true, new Path(getLocalFolder() + localSchemaFile), new Path(schemaFolder + remoteSchemaFile));
		
		//run cmd
		getConf().set("xmlinput.start", "<measInfo>");
		getConf().set("xmlinput.end", "</measInfo>");
		getConf().set("xmlinput.row.start", "<measValue");
		getConf().set("xmlinput.row.end", "</measValue>");
		getConf().set("xmlinput.row.max.number", "3");
		oldMapTest(inputFolder, outputFolder, inputFiles, etl.mapred.util.XmlInputFormat.class);
		//
		List<String> files = HdfsUtil.listDfsFile(getFs(), outputFolder);
		logger.info(String.format("files:\n%s", String.join("\n", files)));
	}
}
