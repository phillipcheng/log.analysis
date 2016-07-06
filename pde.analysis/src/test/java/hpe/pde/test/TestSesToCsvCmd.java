package hpe.pde.test;

import static org.junit.Assert.assertTrue;

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

import etl.cmd.test.TestETLCmd;
import etl.engine.InvokeMapper;
import etl.util.Util;
import hpe.pde.cmd.SesToCsvCmd;

public class TestSesToCsvCmd extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestSesToCsvCmd.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	@Test
	public void test1(){
		try{
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteSesFolder = "/etltest/sestransform/";
			String remoteSeedFolder = "/etltest/sesseed/";
			String remoteCsvOutputFolder = "/etltest/sestransformoutput/";
			//setup testing env
			String csvtransProp = "pde.ses2csv.properties";
			String sesFile = "PK020000_BBG2.ses";
			String seedFile = "ses.seed";
			
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			getFs().delete(new Path(remoteSesFolder), true);
			getFs().delete(new Path(remoteSeedFolder), true);
			
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteSesFolder));
			getFs().mkdirs(new Path(remoteSeedFolder));
			
			getFs().copyFromLocalFile(new Path(getLocalFolder()+csvtransProp), new Path(remoteCfgFolder+csvtransProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+sesFile), new Path(remoteSesFolder+sesFile));
			getFs().copyFromLocalFile(new Path(getLocalFolder()+seedFile), new Path(remoteSeedFolder+seedFile));
			//run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, "hpe.pde.cmd.SesToCsvCmd");
			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder+csvtransProp);
			getConf().set("mapreduce.input.lineinputformat.linespermap", "1");
			Job job = Job.getInstance(getConf(), "testCsvTransformCmd");
			job.setMapperClass(etl.engine.InvokeMapper.class);
			job.setNumReduceTasks(0);//no reducer
			job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job, new Path(remoteSeedFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteCsvOutputFolder));
			job.waitForCompletion(true);
			
			//assertion
			List<String> output = Util.getMROutput(getFs(), remoteCsvOutputFolder);
			assertTrue(output.size()>0);
			String sampleOutput = output.get(0);
			String[] csvs = sampleOutput.split(",");
			logger.info(output);
			assertTrue("BBG2".equals(csvs[csvs.length-1])); //check filename appended to last
		} catch (Exception e) {
			logger.error("", e);
		}
	}
}
