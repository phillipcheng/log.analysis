package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.util.DBType;
import etl.util.DBUtil;

public class SQLExecutionCmd extends SchemaETLCmd {

	private static final long serialVersionUID = 6453410230055894303L;


	public static final Logger logger = LogManager.getLogger(SQLExecutionCmd.class);
	
	
	//cfgkey
	public static final String cfgkey_sql="sql";
	
	private List<String> sqlList;
	
	
	public SQLExecutionCmd(){
		super();
	}
	
	public SQLExecutionCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public SQLExecutionCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		String[] sqls=super.getCfgStringArray(cfgkey_sql);
		if(sqls!=null && sqls.length>0){
			logger.info("Will execute following SQL:\n{}", String.join("\n", sqls));
			sqlList=Arrays.asList(sqls);
		}else{
			logger.error("No SQL is specified!");
			throw new IllegalArgumentException("SQL parameter is mandator!");
		}
		
	}
	
	@Override
	public List<String> sgProcess() {
		List<String> logInfo = new ArrayList<String>();
		if (super.getDbtype()!=DBType.NONE){
			int rowsAdded = DBUtil.executeSqls(sqlList, super.getPc());
			logInfo.add(rowsAdded+"");
		}
		
		return  logInfo;
	}
}
