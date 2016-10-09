package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.script.CompiledScript;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import etl.engine.ETLCmd;
import etl.engine.JoinType;
import etl.engine.MRMode;
import etl.util.ScriptEngineUtil;
import etl.util.StringUtil;
import etl.util.VarType;
import scala.Tuple2;

public class CsvMergeCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(CsvMergeCmd.class);
	
	public static final String KEY_HEADER_SEP="----";
	
	//cfgkey
	public static final String cfgkey_src_files="src.files";
	public static final String cfgkey_src_keys="src.keys";
	public static final String cfgkey_src_skipHeader="src.skipHeader";
	public static final String cfgkey_join_type="join.type";
	public static final String cfgkey_ret_value="ret.value";
	//system varaibles
	public static final String KEY_SYSTEM_VAR_TABLE="table";
	public static final String KEY_SYSTEM_VAR_CSV="csv";
	
	//
	private Pattern[] srcFilesExp;
	private int[] srcKeys;
	private boolean[] srcSkipHeader;
	private JoinType joinType;
	private transient CompiledScript retValueExp;
	private int srcNum;
	
	public CsvMergeCmd(){
		super();
	}
	
	public CsvMergeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvMergeCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		this.setMrMode(MRMode.line);
		String[] srcFiles =super.getCfgStringArray(cfgkey_src_files);
		srcNum = srcFiles.length;
		srcFilesExp = new Pattern[srcNum];
		for (int i=0; i<srcNum; i++){
			String scriptExp = srcFiles[i];
			String globExp = (String) ScriptEngineUtil.eval(scriptExp, VarType.STRING, super.getSystemVariables());
			srcFilesExp[i] = Pattern.compile(StringUtil.convertGlobToRegEx(globExp));
		}
		String[] srcKeysArr = super.getCfgStringArray(cfgkey_src_keys);
		srcKeys = new int[srcNum];
		for (int i=0; i<srcNum; i++){
			srcKeys[i] = Integer.parseInt(srcKeysArr[i]);
		}
		String[] srcSkipHeaderArr = super.getCfgStringArray(cfgkey_src_skipHeader);
		srcSkipHeader = new boolean[srcNum];
		for (int i=0; i<srcNum; i++){
			srcSkipHeader[i] = Boolean.parseBoolean(srcSkipHeaderArr[i]);
		}
		joinType = JoinType.valueOf(super.getCfgString(cfgkey_join_type, JoinType.inner.toString()));
		String retValueStr = super.getCfgString(cfgkey_ret_value, null);
		if (retValueStr!=null){
			retValueExp = ScriptEngineUtil.compileScript(retValueStr);
		}
	}
	
	/**
	 * map function in map-only or map-reduce mode, for map mode: output null for no key or value
	 * @return map may contains following key:
	 * ETLCmd.RESULT_KEY_LOG: list of String user defined log info
	 * ETLCmd.RESULT_KEY_OUTPUT: list of String output
	 * ETLCmd.RESULT_KEY_OUTPUT_MAP: list of Tuple2<key, value>
	 * in the value map, if it contains only 1 value, the key should be ETLCmd.RESULT_KEY_OUTPUT
	 */
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		String fileName = Path.getPathWithoutSchemeAndAuthority(((FileSplit) context.getInputSplit()).getPath()).toString();
		Map<String, Object> retMap = new HashMap<String, Object>();
		int idx=-1;
		for (int i=0; i<srcFilesExp.length; i++){
			if (srcFilesExp[i].matcher(fileName).matches()){
				idx=i;
				break;
			}
		}
		if (idx==-1){
			logger.error(String.format("%s matches nothing %s", fileName, Arrays.asList(srcFilesExp)));
			return retMap;
		}
		if (srcSkipHeader[idx] && offset==0){
			return retMap;
		}
		int keyIdx = srcKeys[idx];
		try {
			CSVParser parser = CSVParser.parse(row, CSVFormat.DEFAULT);
			CSVRecord csv = parser.getRecords().get(0);
			String key = csv.get(keyIdx);
			String value = String.format("%d%s%s", idx, KEY_HEADER_SEP, row);
			Tuple2<String, String> v = new Tuple2<String, String>(key, value);
			List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
			vl.add(v);
			retMap.put(RESULT_KEY_OUTPUT_TUPLE2, vl);
		}catch(Exception e){
			logger.error("", e);
		}
		return retMap;
	}
	
	/**
	 * reduce function in map-reduce mode
	 * set baseOutputPath to ETLCmd.SINGLE_TABLE for single table
	 * set newValue to null, if output line results
	 * @return list of newKey, newValue, baseOutputPath
	 */
	public List<String[]> reduceProcess(Text key, Iterable<Text> values){
		List<String[]> retlist = new ArrayList<String[]>();
		Iterator<Text> it = values.iterator();
		String[] contents = new String[srcNum];
		while (it.hasNext()){
			String v = it.next().toString();
			int keyIdx = Integer.parseInt(v.substring(0, v.indexOf(KEY_HEADER_SEP)));
			String rv = v.substring(v.indexOf(KEY_HEADER_SEP)+KEY_HEADER_SEP.length());
			if (keyIdx<srcNum){
				if (contents[keyIdx]==null){
					contents[keyIdx]=rv;
				}else{
					logger.warn(String.format("duplicated key, exist %s, now come:%s", contents[keyIdx], rv));
				}
			}else{
				logger.error(String.format("keyIdx:%d >= srcNum:%d", keyIdx, srcNum));
			}
		}
		if (JoinType.inner==joinType){
			boolean allFull=true;
			int emptyIdx=0;
			for (int i=0; i<srcNum; i++){
				if (contents[i]==null){
					allFull = false;
					emptyIdx = i;
					break;
				}else{
					this.getSystemVariables().put(KEY_SYSTEM_VAR_TABLE+i, contents[i].split(",", -1));
					this.getSystemVariables().put(KEY_SYSTEM_VAR_CSV+i, contents[i]);
				}
			}
			if (allFull){
				String ret = ScriptEngineUtil.eval(retValueExp, this.getSystemVariables());
				retlist.add(new String[]{ret, null, ETLCmd.SINGLE_TABLE});
			}else{
				logger.warn(String.format("for inner join, data from table %d is empty", emptyIdx));
			}
		}else{
			logger.error(String.format("join type %s not yet supported", joinType));
		}
		
		return retlist;
	}

}
