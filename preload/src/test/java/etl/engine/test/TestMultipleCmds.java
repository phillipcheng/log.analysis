package etl.engine.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
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

public class TestMultipleCmds extends TestETLCmd{
	public static final Logger logger = Logger.getLogger(TestMultipleCmds.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Test
	public void test1(){
		try{
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/multiplecmds/";
			String remoteCsvOutputFolder = "/etltest/multiplecmdsoutput/";
			//setup testing env
			String[] cmdClasses = new String[]{"etl.cmd.CsvTransformCmd","etl.cmd.EvtBasedMsgParseCmd"};
			String[] cmdProperties = new String[]{"hlr.csvtransform.properties","hlr.msgparse.properties"};
			String[] csvFiles = new String[]{"input.hlr1.txt"};
			getFs().mkdirs(new Path(remoteCfgFolder));
			for (String cmdProperty:cmdProperties){
				getFs().copyFromLocalFile(new Path(getLocalFolder()+cmdProperty), new Path(remoteCfgFolder+cmdProperty));
			}
			getFs().delete(new Path(remoteCsvFolder), true);
			getFs().mkdirs(new Path(remoteCsvFolder));
			for (String csvFile:csvFiles){
				getFs().copyFromLocalFile(new Path(getLocalFolder()+csvFile), new Path(remoteCsvFolder+csvFile));
			}
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			
			//run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, StringUtils.join(cmdClasses, ","));
			getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			String[] cmdPropertiesFullPath = new String[cmdProperties.length];
			for (int i=0; i<cmdProperties.length; i++){
				cmdPropertiesFullPath[i]=remoteCfgFolder+cmdProperties[i];
			}
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, StringUtils.join(cmdPropertiesFullPath, ","));
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

	@Override
	public String getResourceSubFolder() {
		return "multiplecmds/";
	}
}
