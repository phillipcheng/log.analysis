package etl.cmd.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.engine.InvokeMapper;

public class TestCsvTransformCmd {
	public static final Logger logger = Logger.getLogger(TestCsvTransformCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	@Test
	public void test1(){
		try{
			Configuration conf = new Configuration();
			String defaultFS = "hdfs://127.0.0.1:19000";
			conf.set("fs.defaultFS", defaultFS);
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/csvtransform/";
			String remoteCsvOutputFolder = "/etltest/csvtransformout/";
			FileSystem fs = FileSystem.get(conf);
			//setup testing env
			String localResourceFolder = "/Users/chengyi/git/log.analysis/preload/src/test/resources/";
			String csvtransProp = "csvtrans.properties";
			String csvFile = "PJ24002A_BBG2.csv";
			fs.mkdirs(new Path(remoteCfgFolder));
			fs.mkdirs(new Path(remoteCsvFolder));
			fs.copyFromLocalFile(new Path(localResourceFolder+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
			fs.copyFromLocalFile(new Path(localResourceFolder+csvFile), new Path(remoteCsvFolder+csvFile));
			fs.delete(new Path(remoteCsvOutputFolder), true);
			//run job
			conf.set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.CsvTransformCmd");
			conf.set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			conf.set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder+csvtransProp);
			Job job = Job.getInstance(conf, "testCsvTransformCmd");
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
