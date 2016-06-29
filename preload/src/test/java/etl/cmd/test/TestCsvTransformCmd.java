package etl.cmd.test;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.engine.InvokeMapper;
import etl.util.Util;

public class TestCsvTransformCmd extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestCsvTransformCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Test
	public void testFileNameRowValidationSkipHeader(){
		try{
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			//setup testing env
			String csvtransProp = "csvtrans.properties";
			String[] csvFiles = new String[]{"PJ24002A_BBG2.csv", "PJ24002B_BBG2.csv"};
			getFs().delete(new Path(remoteCsvFolder), true);
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteCsvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
			for (String csvFile:csvFiles){
				getFs().copyFromLocalFile(new Path(getLocalFolder()+csvFile), new Path(remoteCsvFolder+csvFile));
			}
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			//run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.CsvTransformCmd");
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
			
			//assertion
			List<String> output = Util.getMROutput(getFs(), remoteCsvOutputFolder);
			assertTrue(output.size()>0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			assertTrue("BBG2".equals(csvs[csvs.length-1])); //check filename appended to last
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	@Test
	public void testMergeCol(){
		try{
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			//setup testing env
			String csvtransProp = "csvtrans2.properties";
			String csvFile = "csvtrans2.csv";
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteCsvFolder));
			getFs().delete(new Path(remoteCsvFolder), true);
			getFs().copyFromLocalFile(new Path(getLocalFolder()+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+csvFile), new Path(remoteCsvFolder+csvFile));
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			//run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.CsvTransformCmd");
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
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
