package etl.cmd;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import etl.engine.ETLCmd;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class HdfsCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(HdfsCmd.class);
	
	//cfgkey
	public static final String cfg_key_rm_folders = "rm.folders";
	
	private String[] rmFolders;
	
	public HdfsCmd(){
		super();
	}
	
	public HdfsCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public HdfsCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		String[] rmFolderExps = super.getCfgStringArray(cfg_key_rm_folders);
		rmFolders = new String[rmFolderExps.length];
		for (int i=0; i<rmFolderExps.length; i++){
			rmFolders[i] = (String) ScriptEngineUtil.eval(rmFolderExps[i], VarType.STRING, super.getSystemVariables());
		}
	}

	@Override
	public List<String> sgProcess(){
		try {
			for (String rmFolder: rmFolders){
				super.getFs().delete(new Path(rmFolder), true);
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return Arrays.asList(new String[]{String.valueOf(rmFolders.length)});
	}
	

	public String[] getRmFolders() {
		return rmFolders;
	}

	public void setRmFolders(String[] rmFolders) {
		this.rmFolders = rmFolders;
	}
}
