package etl.engine;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public abstract class FileETLCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(FileETLCmd.class);

	public static final String cfgkey_add_filename="add.filename";
	public static final String cfgkey_exp_filename="exp.filename";//filename extraction expression
	
	public static final String VAR_NAME_FILE_NAME="filename";
	public static final String KEY_SEP = ",";
	
	private boolean addFileName = false;
	private String expFileName = null;
	
	public FileETLCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, defaultFs, otherArgs);
		addFileName = pc.getBoolean(cfgkey_add_filename, false);
		expFileName = pc.getString(cfgkey_exp_filename);
	}
	
	public String getAbbreFileName(String filename){
		Map<String,Object> variables = new HashMap<String, Object>();
		variables.put(VAR_NAME_FILE_NAME, filename);
		String abbr = (String) ScriptEngineUtil.eval(expFileName, VarType.STRING, variables);
		return abbr;
	}
	
	public boolean isAddFileName(){
		return addFileName;
	}
}
