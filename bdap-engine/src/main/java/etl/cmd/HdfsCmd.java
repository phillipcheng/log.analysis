package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.script.CompiledScript;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.HdfsUtil;
import etl.engine.ETLCmd;
import etl.engine.HdfsOp;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;
import scala.Tuple3;

public class HdfsCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(HdfsCmd.class);
	
	public static final String VAR_NAME_FROM_PATH="FROM_PATH";
	//cfgkey for sg-process
	public static final @ConfigKey(type=String[].class) String cfgkey_rm_folders = "rm.folders";
	public static final @ConfigKey(type=String[].class) String cfgkey_mkdir_folders = "mkdir.folders";
	public static final @ConfigKey(type=String[].class) String cfgkey_mv_from = "mv.from";
	public static final @ConfigKey(type=String[].class) String cfgkey_mv_to = "mv.to";
	//cfgkey for mr/spark
	public static final @ConfigKey(type=HdfsOp.class) String cfgkey_op="hdfs.op";
	public static final @ConfigKey(type=String.class) String cfgkey_rm_parent="rm.parent";
	public static final @ConfigKey(type=Integer.class) String cfgkey_chmod_permissions="chmod.permissions";
	public static final @ConfigKey(type=String.class) String cfgkey_mv_to_exp="mv.to.exp";//mv to file name based on mv from file name
	public static final @ConfigKey(type=String.class) String cfgkey_clean_mv_to_folder="clean.mv.to.folder";
	
	//sg var
	private String[] rmFolders;
	private String[] mkdirFolders;
	private String[] mvFrom;
	private String[] mvTo;
	
	//mr var
	private HdfsOp hdfsOp;
	private boolean rmParent=true;
	private String strMvTo;
	private transient CompiledScript csMvTo=null;
	private boolean cleanMvToFolder=false;
	private transient FsPermission chmodPermissions=FsPermission.getDirDefault();
	
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
		String[] mkdirFolderExps = super.getCfgStringArray(cfgkey_mkdir_folders);
		mkdirFolders = new String[mkdirFolderExps.length];
		for (int i=0; i<mkdirFolderExps.length; i++){
			mkdirFolders[i] = (String) ScriptEngineUtil.eval(mkdirFolderExps[i], VarType.STRING, super.getSystemVariables());
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
		String hdfsOpStr = super.getCfgString(cfgkey_op, null);
		if (hdfsOpStr!=null){
			hdfsOp = HdfsOp.valueOf(hdfsOpStr);
		}
		rmParent = super.getCfgBoolean(cfgkey_rm_parent, true);
		if (super.cfgContainsKey(cfgkey_chmod_permissions))
			chmodPermissions = FsPermission.createImmutable(Short.valueOf(super.getCfgString(cfgkey_chmod_permissions, Short.toString(FsPermission.getDirDefault().toShort())), 8));
		strMvTo = super.getCfgString(cfgkey_mv_to_exp, null);
		if (strMvTo!=null){
			csMvTo = ScriptEngineUtil.compileScript(strMvTo);
		}
		cleanMvToFolder = super.getCfgBoolean(cfgkey_clean_mv_to_folder, false);
	}

	@Override
	public List<String> sgProcess(){
		try {
			for (String rmFolder: rmFolders){
				getFs().delete(new Path(rmFolder), true);
			}
			for (String mkdirFolder: mkdirFolders){
				getFs().mkdirs(new Path(mkdirFolder));
				getFs().setPermission(new Path(mkdirFolder), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
			}
			for (int i=0; i<mvFrom.length; i++){
				getFs().rename(new Path(mvFrom[i]), new Path(mvTo[i]));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return Arrays.asList(new String[]{String.valueOf(rmFolders.length + mvFrom.length)});
	}
	
	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception{
		init();
		List<Tuple2<String, String>> mapRet = new ArrayList<Tuple2<String, String>>();
		String filePath = HdfsUtil.getRootPath(value);
		if (HdfsOp.rm == hdfsOp){
			if (rmParent){
				int lastIdx = filePath.lastIndexOf("/");
				String parentPath = filePath.substring(0, lastIdx);
				mapRet.add(new Tuple2<String, String>(parentPath, "1"));
			}else{
				boolean ret = getFs().delete(new Path(filePath), true);
				logger.info(String.format("delete path:%s, ret:%b", filePath, ret));
				mapRet.add(new Tuple2<String, String>(filePath, "1"));
			}
		}else if (HdfsOp.mv == hdfsOp){
			if (this.csMvTo!=null){
				getSystemVariables().put(VAR_NAME_FROM_PATH, filePath);
				String toPath = ScriptEngineUtil.eval(csMvTo, getSystemVariables());
				int lastIdx = toPath.lastIndexOf("/");
				String parentPath = toPath.substring(0, lastIdx);
				HdfsUtil.checkFolder(fs, parentPath, this.cleanMvToFolder);
				boolean ret = getFs().rename(new Path(filePath), new Path(toPath));
				logger.info(String.format("rename path: from: %s, to:%s, ret:%b", filePath, toPath, ret));
				mapRet.add(new Tuple2<String, String>(toPath, "1"));
			}else{
				logger.error(String.format("mv_to_exp not defined!!"));
			}
		}else if (HdfsOp.chmod == hdfsOp){
			int lastIdx = filePath.lastIndexOf("/");
			String parentPath = filePath.substring(0, lastIdx);
			mapRet.add(new Tuple2<String, String>(parentPath, "1"));
		}else{
			logger.error(String.format("unsupported hdfs op in mr mode:%s", hdfsOp));
		}
		return mapRet;
	}
	
	public List<Tuple3<String, String, String>> reduceByKey(String key, Iterable<String> values, 
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception{
		init();
		List<Tuple3<String, String, String>> redRet = new ArrayList<Tuple3<String, String, String>>();
		Iterator<String> it = values.iterator();
		int cnt=0;
		while (it.hasNext()){
			it.next();
			cnt++;
		}
		if (HdfsOp.rm==hdfsOp){
			if (rmParent){
				boolean ret = super.getFs().delete(new Path(key), true);
				logger.info(String.format("delete path:%s, ret:%b", key, ret));
			}
		} else if (HdfsOp.chmod==hdfsOp) {
			super.getFs().setPermission(new Path(key), chmodPermissions);
			logger.info(String.format("change permission of path:%s", key));
		}
		redRet.add(new Tuple3<String,String,String>(key, String.valueOf(cnt), SINGLE_TABLE));
		return redRet;
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

	public String[] getMkdirFolders() {
		return mkdirFolders;
	}

	public void setMkdirFolders(String[] mkdirFolders) {
		this.mkdirFolders = mkdirFolders;
	}
}
