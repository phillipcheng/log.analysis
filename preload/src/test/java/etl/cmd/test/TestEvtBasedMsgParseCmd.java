 package etl.cmd.test;
 
  import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FileSystem;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.io.NullWritable;
  import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
  import org.junit.Test;
  
 import etl.engine.ETLCmd;
  import etl.engine.InvokeMapper;
import etl.util.Util;
  
  public class TestEvtBasedMsgParseCmd extends TestETLCmd{
 	public static final Logger logger = Logger.getLogger(TestEvtBasedMsgParseCmd.class);
 	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
 	
 	@Test
 	public void test1() throws IOException, InterruptedException{
 		try{
 			String remoteCfgFolder = "/etltest/cfg/";
 			String remoteCsvFolder = "/etltest/msgparse/";
 			String remoteCsvOutputFolder = "/etltest/msgparseout/";
 			String csvtransProp = "hlr.msgparse.singlecmd.properties";
 			String localExpectedFile = "expected_parsed_file";
 			String[] csvFiles = new String[]{"input.hlr1.txt"};
 			getFs().mkdirs(new Path(remoteCfgFolder));
 			getFs().mkdirs(new Path(remoteCsvFolder));
 			getFs().delete(new Path(remoteCsvFolder), true);
 			getFs().copyFromLocalFile(new Path(getLocalFolder()+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
 			for (String csvFile:csvFiles){
 				getFs().copyFromLocalFile(new Path(getLocalFolder()+csvFile), new Path(remoteCsvFolder+csvFile));
 			}
 			getFs().delete(new Path(remoteCsvOutputFolder), true);
 			//run job
 			getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.EvtBasedMsgParseCmd");
 			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
 			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder+csvtransProp);
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
 			List<String> output = Util.getMROutput(getFs(), remoteCsvOutputFolder);

 			//check the results match the expected output
 			assertTrue(output.size()==expectedData.size());
 			assertTrue(output.containsAll(expectedData));
 			logger.info("The results verified succesfully.!");	
		} catch (Exception e) {
 			logger.error("", e);
 		}
	}
 		
		@Test
		public void remoteTest1() throws Exception{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					test1();
					return null;
				}
			});

		}
 }