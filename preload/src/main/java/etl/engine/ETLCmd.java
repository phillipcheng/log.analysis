package etl.engine;

import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import etl.cmd.BackupCmd;
import etl.util.Util;

public abstract class ETLCmd {
	public static final Logger logger = Logger.getLogger(ETLCmd.class);
	
	protected String wfid;
	protected FileSystem fs;
	protected Map<String, List<String>> dynCfgMap;
	protected PropertiesConfiguration pc;
	protected String outDynCfg;
	private Configuration conf;
	
	public Configuration getHadoopConf(){
		return conf;
	}
	
	public ETLCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		this.wfid = wfid;
		try {
			conf = new Configuration();
			if (defaultFs!=null){
				conf.set("fs.defaultFS", defaultFs);
			}
			this.fs = FileSystem.get(conf);
		}catch(Exception e){
			logger.error("", e);
		}
		if (inDynCfg!=null){
			dynCfgMap = (Map<String, List<String>>) Util.fromDfsFile(fs, inDynCfg, Map.class);
		}
		this.pc = Util.getPropertiesConfigFromDfs(fs, staticCfg);
		this.outDynCfg = outDynCfg;
	}
	
	public abstract void process(String param);
}
