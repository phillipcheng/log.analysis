package etl.flow.test.cmd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import etl.cmd.CsvAggregateCmd;
import etl.cmd.CsvTransformCmd;
import etl.cmd.SaveDataCmd;
import etl.cmd.SftpCmd;
import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.SparkUtil;

public class Flow1SparkCmd extends ETLCmd implements Serializable{
	private static final long serialVersionUID = 1L;
	public static final Logger logger = Logger.getLogger(Flow1SparkCmd.class);

	private static @ConfigKey String keycfg_resfolder="res.folder";//specify res.folder under classpath

	private String masterUrl=null;
	private String resFolder;
	
	public Flow1SparkCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.resFolder = super.getCfgString(keycfg_resfolder, "");
	}
	
	@Override
	public List<String> sgProcess() throws Exception{
		List<String> retInfo = new ArrayList<String>();

		String defaultFs = super.getDefaultFs();
		
		SparkConf conf = new SparkConf().setAppName(getWfName());
		if (masterUrl!=null){
			conf.setMaster(masterUrl);
		}
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		//data instance is false, generate initial value
		//record type is Value, generated JavaRDD<String>
		JavaRDD<String> sftpMap= SparkUtil.fromFile(this.getDefaultFs() + "/flow1/sftpcfg/test1.sftp.map.properties", jsc);
		//when a node has multiple outlet, generate a data variable to use filter against
		//record type is KeyValue
		JavaPairRDD<String,String> sftpOutput = null;
		//
		JavaRDD<String> data1;
		JavaRDD<String> data2;
		//
		JavaPairRDD<String,String> data1trans;
		JavaPairRDD<String,String> data2trans;
		//
		JavaPairRDD<String,String> mergeCsvs;
		
		SftpCmd sftpCmd = new SftpCmd(getWfName(), getWfid(), resFolder + "action_sftp.properties", super.getDefaultFs(), null);
		sftpOutput = sftpCmd.sparkProcessV2KV(sftpMap, jsc, TextInputFormat.class, spark);
		data1 = SparkUtil.filterPairRDD(sftpOutput, "data1");
		data2 = SparkUtil.filterPairRDD(sftpOutput, "data2");
		
		CsvTransformCmd d1csvTransformCmd = new CsvTransformCmd(wfName, wfid, resFolder + "action_d1csvtransform.properties", defaultFs, null, ProcessMode.Single);
		data1trans = d1csvTransformCmd.sparkProcessFilesToKV(data1, jsc, TextInputFormat.class, spark);
		
		CsvTransformCmd d2csvTransformCmd = new CsvTransformCmd(wfName, wfid, resFolder + "action_d2csvtransform.properties", defaultFs, null, ProcessMode.Single);
		data2trans = d2csvTransformCmd.sparkProcessFilesToKV(data2, jsc, TextInputFormat.class, spark);
		
		CsvAggregateCmd mergeCmd = new CsvAggregateCmd(wfName, wfid, resFolder + "action_csvaggr.properties", defaultFs, null);
		mergeCsvs = mergeCmd.sparkProcessKeyValue(data1trans.union(data2trans), jsc, TextInputFormat.class, spark);
		
		SaveDataCmd saveCmd = new SaveDataCmd(wfName, wfid, resFolder + "action_csvsave.properties", defaultFs, null);
		saveCmd.sparkProcessKeyValue(mergeCsvs, jsc, TextInputFormat.class, spark);
		
		jsc.close();
		
		return retInfo;
	}

	public String getResFolder() {
		return resFolder;
	}

	public void setResFolder(String resFolder) {
		this.resFolder = resFolder;
	}
	
	public String getMasterUrl() {
		return masterUrl;
	}

	public void setMasterUrl(String masterUrl) {
		this.masterUrl = masterUrl;
	}
}
