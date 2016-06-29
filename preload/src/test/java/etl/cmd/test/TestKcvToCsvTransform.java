package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

import etl.cmd.SftpCmd;
import etl.cmd.transform.KcvToCsvCmd;
import etl.engine.InvokeMapper;
import etl.util.Util;

public class TestKcvToCsvTransform extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestKcvToCsvTransform.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	@Test
	public void testSuccess() throws Exception {
		UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				BufferedReader br = null;
				try {
					String remoteCfgFolder = "/etltest/cfg/";
					String remoteCsvFolder = "/etltest/kcvtransform/";
					String remoteCsvOutputFolder = "/etltest/kcvtransformout/";
					// setup testing env
					String csvtransProp = "kcv2csv.properties";
					String kcvFile = "PJ24002A_BBG2.fix";
					String line = null;
					getFs().mkdirs(new Path(remoteCfgFolder));
					getFs().mkdirs(new Path(remoteCsvFolder));
					getFs().copyFromLocalFile(new Path(getLocalFolder() + csvtransProp),
							new Path(remoteCfgFolder + csvtransProp));
					getFs().copyFromLocalFile(new Path(getLocalFolder() + kcvFile),
							new Path(remoteCsvFolder + kcvFile));
					getFs().delete(new Path(remoteCsvOutputFolder), true);
					// run job
					KcvToCsvCmd cmd = new KcvToCsvCmd(null, remoteCfgFolder + csvtransProp, null, null, getDefaultFS());
					List<String> retlist = cmd.process(0, kcvFile, null);
					logger.info(retlist);
					assertTrue(retlist != null);

					br = new BufferedReader(new InputStreamReader(getFs().open(new Path(remoteCsvFolder + kcvFile))));
					if ((line = br.readLine()) != null) {
						logger.info(line);
						logger.info(retlist.get(0).trim());
					}
				} catch (Exception e) {
					logger.error("", e);
				} finally {
					if (br != null)
						br.close();
				}
				return null;
			}
		});
	}

	/*
	 * @Test public void testMapReduce(){ try{ String remoteCfgFolder =
	 * "/etltest/cfg/"; String remoteCsvFolder = "/etltest/kcvtransform/";
	 * String remoteCsvOutputFolder = "/etltest/kcvtransformout/"; //setup
	 * testing env String csvtransProp = "kcv2csv.properties"; String[] csvFiles
	 * = new String[]{"PJ24002A_BBG2.fix", "PJ24002B_BBG2.fix"};
	 * getFs().delete(new Path(remoteCsvFolder), true); getFs().delete(new
	 * Path(remoteCfgFolder), true); getFs().delete(new
	 * Path(remoteCsvOutputFolder), true); getFs().mkdirs(new
	 * Path(remoteCfgFolder)); getFs().mkdirs(new Path(remoteCsvFolder));
	 * getFs().mkdirs(new Path(remoteCsvOutputFolder));
	 * getFs().copyFromLocalFile(new Path(getLocalFolder()+csvtransProp), new
	 * Path(remoteCfgFolder+csvtransProp)); for (String csvFile:csvFiles){
	 * getFs().copyFromLocalFile(new Path(getLocalFolder()+csvFile), new
	 * Path(remoteCsvFolder+csvFile)); } //run job
	 * getConf().set(InvokeMapper.cfgkey_cmdclassname,
	 * "etl.cmd.transform.KcvToCsvCmd"); getConf().set(InvokeMapper.cfgkey_wfid,
	 * sdf.format(new Date()));
	 * getConf().set(InvokeMapper.cfgkey_staticconfigfile,
	 * remoteCfgFolder+csvtransProp); Job job = Job.getInstance(getConf(),
	 * "testKcvToCsvCmd"); job.setMapperClass(etl.engine.InvokeMapper.class);
	 * job.setNumReduceTasks(0);//no reducer job.setOutputKeyClass(Text.class);
	 * job.setOutputValueClass(NullWritable.class);
	 * FileInputFormat.setInputDirRecursive(job, true);
	 * FileInputFormat.addInputPath(job, new Path(remoteCsvFolder));
	 * FileOutputFormat.setOutputPath(job, new Path(remoteCsvOutputFolder));
	 * //job.waitForCompletion(true);
	 * 
	 * //assertion List<String> output = Util.getMROutput(getFs(),
	 * remoteCsvOutputFolder); assertTrue(output.size()>0); String sampleOutput
	 * = output.get(0); String[] csvs = sampleOutput.split(",");
	 * assertTrue("BBG2".equals(csvs[csvs.length-1])); //check filename appended
	 * to last } catch (Exception e) { logger.error("", e); } }
	 */

}
