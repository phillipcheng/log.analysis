package etl.cmd.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
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

public class TestMultipleCmds {
	public static final Logger logger = Logger.getLogger(TestMultipleCmds.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Test
	public void test1(){
		try{
			Configuration conf = new Configuration();
			String defaultFS = "hdfs://127.0.0.1:19000";
			conf.set("fs.defaultFS", defaultFS);
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/multiplecmds/";
			String remoteCsvOutputFolder = "/etltest/multiplecmdsoutput/";
			FileSystem fs = FileSystem.get(conf);
			//setup testing env
			//String localResourceFolder = "/Users/chengyi/git/log.analysis/preload/src/test/resources/";
			String localResourceFolder = "C:\\mydoc\\myprojects\\log.analysis\\preload\\src\\test\\resources\\";
			String[] cmdClasses = new String[]{"etl.cmd.transform.CsvTransformCmd","etl.cmd.transform.EvtBasedMsgParseCmd"};
			String[] cmdProperties = new String[]{"hlr.csvtransform.properties","hlr.msgparse.properties"};
			String[] csvFiles = new String[]{"input.hlr1.txt"};
			fs.mkdirs(new Path(remoteCfgFolder));
			for (String cmdProperty:cmdProperties){
				fs.copyFromLocalFile(new Path(localResourceFolder+cmdProperty), new Path(remoteCfgFolder+cmdProperty));
			}
			fs.delete(new Path(remoteCsvFolder), true);
			fs.mkdirs(new Path(remoteCsvFolder));
			for (String csvFile:csvFiles){
				fs.copyFromLocalFile(new Path(localResourceFolder+csvFile), new Path(remoteCsvFolder+csvFile));
			}
			fs.delete(new Path(remoteCsvOutputFolder), true);
			
			//run job
			conf.set(InvokeMapper.cfgkey_cmdclassname, StringUtils.join(cmdClasses, ","));
			conf.set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			String[] cmdPropertiesFullPath = new String[cmdProperties.length];
			for (int i=0; i<cmdProperties.length; i++){
				cmdPropertiesFullPath[i]=remoteCfgFolder+cmdProperties[i];
			}
			conf.set(InvokeMapper.cfgkey_staticconfigfile, StringUtils.join(cmdPropertiesFullPath, ","));
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
