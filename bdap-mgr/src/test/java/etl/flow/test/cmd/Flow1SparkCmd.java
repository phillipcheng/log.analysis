package etl.flow.test.cmd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import etl.engine.ETLCmd;

public class Flow1SparkCmd extends ETLCmd implements Serializable {
	public static final Logger logger = Logger.getLogger(Flow1SparkCmd.class);
	private String masterUrl = null;
	private String resFolder = "";

	public void setResFolder(String resFolder) {
		this.resFolder = resFolder;
	}

	public void setMasterUrl(String masterUrl) {
		this.masterUrl = masterUrl;
	}

	public Flow1SparkCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, etl.engine.types.ProcessMode pm){
	init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, etl.engine.types.ProcessMode.Single);}

	public List<String> sgProcess() {
		List<String> retInfo = new ArrayList<String>();
		SparkConf conf = new SparkConf().setAppName(getWfName());
		if (masterUrl != null)
			conf.setMaster(masterUrl);
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaPairRDD<String, String> sftp_map = etl.util.SparkUtil.fromFileKeyValue(
				this.getDefaultFs() + "/flow1/sftpcfg/test1.sftp.map.properties", jsc, getHadoopConf(), null);
		JavaPairRDD<String, String> data1 = null;
		JavaPairRDD<String, String> data2 = null;
		JavaPairRDD<String, String> data1trans = null;
		JavaPairRDD<String, String> data2trans = null;
		JavaPairRDD<String, String> csvmerge = null;
		JavaPairRDD<String, String> csvmergefiles = null;
		JavaPairRDD<String, String> csvdbout = null;
		etl.cmd.SftpCmd sftpCmd = new etl.cmd.SftpCmd(getWfName(), getWfid(), resFolder + "action_sftp.properties",
				super.getDefaultFs(), null);
		JavaPairRDD<String, String> sftpOutput = null;
		sftpOutput = sftpCmd.sparkProcessKeyValue(sftp_map, jsc, etl.engine.types.InputFormatType.Line, spark);
		sftpOutput.cache();
		data1 = etl.util.SparkUtil.filterPairRDD(sftpOutput, "data1");
		data2 = etl.util.SparkUtil.filterPairRDD(sftpOutput, "data2");
		etl.cmd.CsvTransformCmd d1csvtransformCmd = new etl.cmd.CsvTransformCmd(getWfName(), getWfid(),
				resFolder + "action_d1csvtransform.properties", super.getDefaultFs(), null);
		data1trans = d1csvtransformCmd.sparkProcessFilesToKV(data1, jsc, etl.engine.types.InputFormatType.Text, spark);
		etl.flow.test.cmd.Flow1Xml2CsvCmd d2xml2csvCmd = new etl.flow.test.cmd.Flow1Xml2CsvCmd(getWfName(), getWfid(),
				resFolder + "action_d2xml2csv.properties", super.getDefaultFs(), null);
		data2trans = d2xml2csvCmd.sparkProcessFilesToKV(data2, jsc, etl.engine.types.InputFormatType.CombineXML, spark);
		etl.cmd.CsvAggregateCmd csvmergeCmd = new etl.cmd.CsvAggregateCmd(getWfName(), getWfid(),
				resFolder + "action_csvmerge.properties", super.getDefaultFs(), null);
		csvmerge = csvmergeCmd.sparkProcessKeyValue(data1trans.union(data2trans), jsc,
				etl.engine.types.InputFormatType.Text, spark);
		etl.cmd.SaveDataCmd csvsaveCmd = new etl.cmd.SaveDataCmd(getWfName(), getWfid(),
				resFolder + "action_csvsave.properties", super.getDefaultFs(), null);
		csvmergefiles = csvsaveCmd.sparkProcessKeyValue(csvmerge, jsc, etl.engine.types.InputFormatType.Text, spark);
		etl.cmd.LoadDataCmd loadcsvCmd = new etl.cmd.LoadDataCmd(getWfName(), getWfid(),
				resFolder + "action_loadcsv.properties", super.getDefaultFs(), null);
		csvdbout = loadcsvCmd.sparkProcessKeyValue(csvmergefiles, jsc, etl.engine.types.InputFormatType.Text, spark);
		logger.info("csvdbout:" + csvdbout.collect());
		jsc.close();
		return retInfo;
	}
}
