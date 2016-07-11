package etl.cmd.test;

import static org.junit.Assert.*;

import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.engine.InvokeMapper;
import etl.util.Util;

public class TestCsvTransformCmd extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestCsvTransformCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	private void testFileNameRowValidationSkipHeaderFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			// setup testing env
			String csvtransProp = "csvtrans.properties";
			String[] csvFiles = new String[] {"PJ24002A_BBG2.csv"};
			int mergedColumn = 2;
			
			getFs().delete(new Path(remoteCsvFolder), true);
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteCsvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvtransProp), new Path(remoteCfgFolder + csvtransProp));
			for (String csvFile : csvFiles) {
				getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile), new Path(remoteCsvFolder + csvFile));
			}
			// run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.CsvTransformCmd");
			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder + csvtransProp);
			Job job = Job.getInstance(getConf(), "testCsvTransformCmd");
			job.setMapperClass(etl.engine.InvokeMapper.class);
			job.setNumReduceTasks(0);// no reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job, new Path(remoteCsvFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteCsvOutputFolder));
			job.waitForCompletion(true);

			// assertion
			List<String> output = Util.getMROutput(getFs(), remoteCsvOutputFolder);
			logger.info("Output is:"+output);
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			logger.info("Last element:"+csvs[csvs.length - 1]+" "+ csvs[mergedColumn] + " "+csvs[mergedColumn-1]+ " " +csvs[mergedColumn+1]);
			assertTrue("BBG2".equals(csvs[csvs.length - 1])); // check filename appended to last
			assertFalse("MeasTime".equals(csvs[0]));//skip header check
			assertTrue(csvs[mergedColumn].contains("-"));//check column merged
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testFileNameRowValidationSkipHeader() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testFileNameRowValidationSkipHeaderFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testFileNameRowValidationSkipHeaderFun();
					return null;
				}
			});
		}
	}
	

	
	private void testSplitColFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			// setup testing env
			String csvtransProp = "csvtrans3.properties";
			String csvFile = "csvtrans2.csv";
			int splitColumn = 2;
			getFs().delete(new Path(remoteCsvFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteCsvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvtransProp),
					new Path(remoteCfgFolder + csvtransProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile),
					new Path(remoteCsvFolder + csvFile));
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			// run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.CsvTransformCmd");
			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder + csvtransProp);
			Job job = Job.getInstance(getConf(), "testCsvTransformCmd");
			job.setMapperClass(etl.engine.InvokeMapper.class);
			job.setNumReduceTasks(0);// no reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job, new Path(remoteCsvFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteCsvOutputFolder));
			job.waitForCompletion(true);
			
			List<String> output = Util.getMROutput(getFs(), remoteCsvOutputFolder);
			logger.info("Output is:"+output);
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			logger.info("Updated Column value"+csvs[splitColumn]+" "+ csvs[splitColumn+1] + " "+csvs[splitColumn+2]);
			assertFalse(csvs[splitColumn].contains("."));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	@Test
	public void testSplitCol() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testSplitColFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testSplitColFun();
					return null;
				}
			});
		}
	}
	
	private void testUpdateColFun() throws Exception {
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			// setup testing env
			String csvtransProp = "csvtrans2.properties";
			String csvFile = "csvtrans2.csv";
			int updateColumn1 = 2;
			int updateColumn2 = 3;
			String replaceString1 = ".";
			String replaceString2 = "coarse";
			
			getFs().delete(new Path(remoteCsvFolder), true);
			getFs().delete(new Path(remoteCfgFolder), true);
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteCsvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvtransProp),
					new Path(remoteCfgFolder + csvtransProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + csvFile),
					new Path(remoteCsvFolder + csvFile));
			// run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.CsvTransformCmd");
			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder + csvtransProp);
			Job job = Job.getInstance(getConf(), "testCsvTransformCmd");
			job.setMapperClass(etl.engine.InvokeMapper.class);
			job.setNumReduceTasks(0);// no reducer
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job, new Path(remoteCsvFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteCsvOutputFolder));
			job.waitForCompletion(true);
			
			List<String> output = Util.getMROutput(getFs(), remoteCsvOutputFolder);
			logger.info("Output is:"+output);
			assertTrue(output.size() > 0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			assertFalse(csvs[updateColumn1].contains(replaceString1.trim())); 
			assertFalse(csvs[updateColumn2].contains(replaceString2.trim()));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testUpdateCol() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			testUpdateColFun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					testUpdateColFun();
					return null;
				}
			});
		}
	}
}
