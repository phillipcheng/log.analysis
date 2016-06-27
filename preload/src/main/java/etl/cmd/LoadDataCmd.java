package etl.cmd;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.DBUtil;

public class LoadDataCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(LoadDataCmd.class);
	
	public static final String cfgkey_webhdfs="hdfs.webhdfs.root";
	public static final String cfgkey_csvfolder = "csv.folder";
	public static final String cfgkey_use_wfid = "use.wfid";
	public static final String cfgkey_load_sql = "load.sql";
	
	private String webhdfsRoot;
	private String userName;
	private String csvFolder;
	private boolean useWfid;
	private String loadSql;
	
	public LoadDataCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.csvFolder = pc.getString(cfgkey_csvfolder);
		this.webhdfsRoot = pc.getString(cfgkey_webhdfs);
		this.userName = pc.getString(DBUtil.key_db_user);
		this.csvFolder = pc.getString(cfgkey_csvfolder);
		this.useWfid = pc.getBoolean(cfgkey_use_wfid);
		this.loadSql = pc.getString(cfgkey_load_sql);
	}

	@Override
	public List<String> process(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		List<String> copysqls = new ArrayList<String>();
		if (useWfid){
			String sql = String.format("%s SOURCE Hdfs(url='%s%s%s/part-*',username='%s') delimiter ','", loadSql, webhdfsRoot, csvFolder, wfid, userName);
			logger.info("sql:" + sql);
			copysqls.add(sql);
		}
		DBUtil.executeSqls(copysqls, pc);
		return null;
	}
}