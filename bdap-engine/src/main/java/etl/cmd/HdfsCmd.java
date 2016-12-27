package etl.cmd;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class HdfsCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(HdfsCmd.class);
	
	//cfgkey
	public static final String cfgkey_rm_folders = "rm.folders";
	public static final String cfgkey_mv_from = "mv.from";
	public static final String cfgkey_mv_to = "mv.to";
	
	private String[] rmFolders;
	private String[] mvFrom;
	private String[] mvTo;
	
	public HdfsCmd(){
		super();
	}
	
	public HdfsCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public HdfsCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public HdfsCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		String[] rmFolderExps = super.getCfgStringArray(cfgkey_rm_folders);
		rmFolders = new String[rmFolderExps.length];
		for (int i=0; i<rmFolderExps.length; i++){
			rmFolders[i] = (String) ScriptEngineUtil.eval(rmFolderExps[i], VarType.STRING, super.getSystemVariables());
		}
		String[] mvFromExps = super.getCfgStringArray(cfgkey_mv_from);
		String[] mvToExps = super.getCfgStringArray(cfgkey_mv_to);
		if (mvFromExps.length==mvToExps.length){
			mvFrom = new String[mvFromExps.length];
			mvTo = new String[mvFromExps.length];
			for (int i=0; i<mvFromExps.length; i++){
				mvFrom[i] = (String) ScriptEngineUtil.eval(mvFromExps[i], VarType.STRING, super.getSystemVariables());
				mvTo[i] = (String) ScriptEngineUtil.eval(mvToExps[i], VarType.STRING, super.getSystemVariables());
			}
		}else{
			logger.error(String.format("mv folder from and to should have same number:%s, %s", Arrays.asList(mvFrom), Arrays.asList(mvTo)));
		}
	}

	@Override
	public List<String> sgProcess(){
		try {
			for (String rmFolder: rmFolders){
				getFs().delete(new Path(rmFolder), true);
			}
			for (int i=0; i<mvFrom.length; i++){
				getFs().rename(new Path(mvFrom[i]), new Path(mvTo[i]));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return Arrays.asList(new String[]{String.valueOf(rmFolders.length + mvFrom.length)});
	}
	

	public String[] getRmFolders() {
		return rmFolders;
	}

	public void setRmFolders(String[] rmFolders) {
		this.rmFolders = rmFolders;
	}

	public String[] getMvFrom() {
		return mvFrom;
	}

	public String[] getMvTo() {
		return mvTo;
	}
}
