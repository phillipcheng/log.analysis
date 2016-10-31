package etl.cmd.test;
 
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import bdap.util.EngineConf;
import bdap.util.HdfsUtil;
  
public class TestEvtBasedMsgParseCmd extends TestETLCmd{
 	public static final Logger logger = LogManager.getLogger(TestEvtBasedMsgParseCmd.class);
 	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
 	
 	@Test
 	public void test1() throws IOException, InterruptedException{
 		try{
 			String remoteCsvFolder = "/etltest/msgparse/";
 			String remoteCsvOutputFolder = "/etltest/msgparseout/";
 			String csvtransProp = "hlr.msgparse.singlecmd.properties";
 			String localExpectedFile = "expected_parsed_file";
 			String[] csvFiles = new String[]{"input.hlr1.txt"};
 			getFs().mkdirs(new Path(remoteCsvFolder));
 			getFs().delete(new Path(remoteCsvFolder), true);
 			for (String csvFile:csvFiles){
 				getFs().copyFromLocalFile(new Path(getLocalFolder()+csvFile), new Path(remoteCsvFolder+csvFile));
 			}
 			getFs().delete(new Path(remoteCsvOutputFolder), true);
 			//run job
 			getConf().set(EngineConf.cfgkey_cmdclassname, "etl.cmd.EvtBasedMsgParseCmd");
 			getConf().set(EngineConf.cfgkey_wfid, sdf.format(new Date()));
 			getConf().set(EngineConf.cfgkey_staticconfigfile, this.getResourceSubFolder()+csvtransProp);
 			Job job = Job.getInstance(getConf(), "testCsvTransformCmd");
 			job.setMapperClass(etl.engine.InvokeMapper.class);
 			job.setNumReduceTasks(0);//no reducer
 			job.setOutputKeyClass(Text.class);
 			job.setOutputValueClass(NullWritable.class);
 			FileInputFormat.setInputDirRecursive(job, true);
 			FileInputFormat.addInputPath(job, new Path(remoteCsvFolder));
 			FileOutputFormat.setOutputPath(job, new Path(remoteCsvOutputFolder));
 			job.waitForCompletion(true);
 			
 			//check results
 			//Get data from the Standard expected output file
 			ArrayList<String> expectedData=new ArrayList<String>();
 			String line=null;
 			BufferedReader br = new BufferedReader(new FileReader(getLocalFolder() + localExpectedFile));
 			while ((line = br.readLine()) != null) {
 				expectedData.add(line);  
 			}
 			br.close();
 			//Get data from the output parsed file
 			List<String> output = HdfsUtil.stringsFromDfsFolder(getFs(), remoteCsvOutputFolder);

 			//check the results match the expected output
 			assertTrue(output.size()==expectedData.size());
 			assertTrue(output.containsAll(expectedData));
 			logger.info("The results verified succesfully.!");	
		} catch (Exception e) {
 			logger.error("", e);
 			assertTrue(false);
 		}
 	}

	@Override
	public String getResourceSubFolder() {
		return "evtbased/";
	}
}
