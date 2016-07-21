package etl.cmd.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.junit.Test;

import etl.cmd.transform.KcvToCsvCmd;
import etl.engine.ETLCmd;
import etl.engine.InvokeMapper;
import etl.util.Util;

public class TestKcvToCsvTransform extends TestETLCmd {
	public static final Logger logger = Logger.getLogger(TestKcvToCsvTransform.class);
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	private void test1Fun() throws Exception{
		BufferedReader br = null;
		try {
			String remoteCfgFolder = "/etltest/cfg/";
			String remoteCsvFolder = "/etltest/kcvtransform/";
			String remoteCsvOutputFolder = "/etltest/kcvtransformout/";
			// setup testing env
			String kcvtransProp = "kcv2csv.properties";
			String regexPattern = "[\\s]+([A-Za-z0-9\\,\\. ]+)[\\s]+([A-Z][A-Z ]+)";
			String kcvFile = "PJ24002A_BBG2.fix";
			String line = null;
			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteCsvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + kcvtransProp),
					new Path(remoteCfgFolder + kcvtransProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + kcvFile),
					new Path(remoteCsvFolder + kcvFile));
			getFs().delete(new Path(remoteCsvOutputFolder), true);
			// run job
			KcvToCsvCmd cmd = new KcvToCsvCmd(null, remoteCfgFolder + kcvtransProp, null, null, getDefaultFS());
			Map<String, Object> retMap = cmd.mapProcess(0, kcvFile, null);
			List<String> retlist = (List<String>) retMap.get(ETLCmd.RESULT_KEY_OUTPUT);
			logger.info(retlist);
			assertTrue(retlist != null);
			assertTrue(retlist.size() > 0);
			br = new BufferedReader(new InputStreamReader(getFs().open(new Path(remoteCsvFolder + kcvFile))));
			if ((line = br.readLine()) != null) {
				Pattern pattern = Pattern.compile(regexPattern);
				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					assertTrue(retlist.get(0).trim().startsWith(matcher.group(1)));
				}
			}

		} catch (Exception e) {
			logger.error("", e);
		} finally {
			if (br != null)
				br.close();
		}
	}
	
	@Test
	public void test1() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			test1Fun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					test1Fun();
					return null;
				}
			});
		}
	}
	
	private void test2Fun() throws Exception{
		BufferedReader br = null;
		try {
			String remoteCfgFolder = "/etltest/kcvcfg/";
			String remoteSeedFolder = "/etltest/kcvseed/";
			String remoteKcvFolder = "/etltest/kcvtransform/";
			String remoteKcvOutputFolder = "/etltest/kcvtransformout/";
			// setup testing env
			String kcvtransProp = "kcv2csv.properties";
			String kcvFile = "PJ24002A_BBG2.fix";
			String seedFile = "kcvtocsv.seed";
			String regexPattern = "[\\s]+([A-Za-z0-9\\,\\. ]+)[\\s]+([A-Z][A-Z ]+)";
			String line = null;

			getFs().delete(new Path(remoteKcvFolder), true);
			getFs().delete(new Path(remoteSeedFolder), true);
			getFs().delete(new Path(remoteKcvOutputFolder), true);

			getFs().mkdirs(new Path(remoteCfgFolder));
			getFs().mkdirs(new Path(remoteSeedFolder));
			getFs().mkdirs(new Path(remoteKcvFolder));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + kcvtransProp),
					new Path(remoteCfgFolder + kcvtransProp));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + seedFile),
					new Path(remoteSeedFolder + seedFile));
			getFs().copyFromLocalFile(new Path(getLocalFolder() + kcvFile),
					new Path(remoteKcvFolder + kcvFile));

			// run job
			getConf().set(InvokeMapper.cfgkey_cmdclassname, "etl.cmd.transform.KcvToCsvCmd");
			// getConf().set(InvokeMapper.cfgkey_wfid, sdf.format(new Date()));
			getConf().set(InvokeMapper.cfgkey_staticconfigfile, remoteCfgFolder + kcvtransProp);
			Job job = Job.getInstance(getConf(), "testKcvToCsvTransform");
			job.setMapperClass(etl.engine.InvokeMapper.class);
			job.setNumReduceTasks(0);// no reducer
			job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job, new Path(remoteSeedFolder));
			FileOutputFormat.setOutputPath(job, new Path(remoteKcvOutputFolder));
			job.waitForCompletion(true);

			// assertion
			List<String> output = Util.getMROutput(getFs(), remoteKcvOutputFolder);
			assertTrue(output.size() > 0);
			logger.info("Output:" + output);
			assertTrue(output != null);
			assertTrue(output.size() > 0);
			
			br = new BufferedReader(new InputStreamReader(getFs().open(new Path(remoteKcvFolder + kcvFile))));
			if ((line = br.readLine()) != null) {
				Pattern pattern = Pattern.compile(regexPattern);
				Matcher matcher = pattern.matcher(line);
				if (matcher.find()) {
					assertTrue(output.get(0).trim().startsWith(matcher.group(1)));

				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			if (br != null)
				br.close();
		}
	}

	@Test
	public void test2() throws Exception {
		if (getDefaultFS().contains("127.0.0.1")){
			test2Fun();
		}else{
			UserGroupInformation ugi = UserGroupInformation.createProxyUser("dbadmin", UserGroupInformation.getLoginUser());
			ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					test2Fun();
					return null;
				}
			});
		}
	}

}
