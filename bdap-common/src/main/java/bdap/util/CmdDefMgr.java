package bdap.util;

import java.util.HashMap;
import java.util.Map;

public class CmdDefMgr{
	
	private static CmdDef[] allCmdDef = new CmdDef[]{
		new CmdDef("etl.cmd.CsvAggregateCmd", true),
		new CmdDef("etl.cmd.CsvMergeCmd", true),
		new CmdDef("etl.cmd.CsvSplitCmd", true),
		new CmdDef("etl.cmd.CsvTransformCmd", true),
		new CmdDef("etl.cmd.KdvToCsvCmd", false),
		new CmdDef("etl.cmd.SftpCmd", false),
		new CmdDef("etl.cmd.ShellCmd", false),
		new CmdDef("etl.cmd.XmlToCsvCmd", false)
	};
	
	private static CmdDefMgr instance;
	private CmdDefMgr(){
		//forbid constructor
	}
	
	private Map<String, CmdDef> cmdDefMap;
	
	private void init(){
		cmdDefMap = new HashMap<String, CmdDef>();
		for (CmdDef cd:allCmdDef){
			cmdDefMap.put(cd.getClassName(), cd);
		}
	}
	
	public CmdDef getCmdDef(String className){
		if (cmdDefMap==null){
			init();
		}
		return cmdDefMap.get(className);
	}
	
	public static CmdDefMgr getInstance(){
		if (instance==null){
			instance = new CmdDefMgr();
			instance.init();
		}
		return instance;
	}
}