package etl.cmd.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.engine.InvokeMapper;

public class TestEvtBasedMsgParseCmd extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestEvtBasedMsgParseCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Test
	public void test1(){
		try{
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/msgparse/";
			String remoteCsvOutputFolder = "/etltest/msgparseout/";
			String csvtransProp = "hlr.msgparse.singlecmd.properties";
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
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
