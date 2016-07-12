package etl.cmd.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.sun.jersey.server.impl.cdi.Utils;

import etl.engine.FileETLCmd;
import etl.engine.MRMode;
import etl.engine.ProcessMode;
import etl.util.ScriptEngineUtil;
import etl.util.Util;
import etl.util.VarType;

public class CsvTransformCmd extends FileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvTransformCmd.class);
	
	public static final String cfgkey_skip_header="skip.header";
	public static final String cfgkey_row_validation="row.validation";
	public static final String cfgkey_input_endwithcomma="input.endwithcomma";
	public static final String cfgkey_col_op="col.op";
	
	private boolean skipHeader=false;
	private String rowValidation;
	private boolean inputEndWithComma=false;
	List<ColOp> colOpList = new ArrayList<ColOp>();
	
	public CsvTransformCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		skipHeader =pc.getBoolean(cfgkey_skip_header, false);
		rowValidation = pc.getString(cfgkey_row_validation);
		inputEndWithComma = pc.getBoolean(cfgkey_input_endwithcomma, false);
		
		String[] colops = pc.getStringArray(cfgkey_col_op);
		for (String colop:colops){
			ColOp co = new ColOp(colop);
			colOpList.add(co);
		}
		this.setMrMode(MRMode.line);
	}

	@Override
	public Map<String, List<String>> mrProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		Map<String, List<String>> retMap = new HashMap<String, List<String>>();
		String output="";
		
		//process skip header
		if (skipHeader && offset==0) {
			logger.info("skip header:" + row);
			return null;
		}
		
		//get all fiels
		List<String> items = new ArrayList<String>();
		items.addAll(Arrays.asList(row.split(",")));
		
		//process input ends with comma
		if (inputEndWithComma){//remove the last empty item since row ends with comma
			items.remove(items.size()-1);
		}
		
		//process row validation
		if (rowValidation!=null){
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(ColOp.VAR_NAME_fields, items.toArray());
			boolean valid = (Boolean) ScriptEngineUtil.eval(rowValidation, VarType.BOOLEAN, map);
			if (!valid) {
				logger.info("invalid row:" + row);
				return null;
			}
		}
		
		//process operation
		Map<String, Object> vars = new HashMap<String, Object>();
		String[] strItems = new String[items.size()];
		vars.put(ColOp.VAR_NAME_fields, items.toArray(strItems));
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		vars.put(var_name_filename, fileName);
		for (ColOp co: colOpList){
			items = co.process(vars);
			strItems = new String[items.size()];
			vars.put(ColOp.VAR_NAME_fields, items.toArray(strItems));
		}
		output = Util.getCsv(Arrays.asList(strItems), false, false);
		if (isAddFileName()){
			output+="," + getAbbreFileName(fileName);
		}
		logger.debug("output:" + output);
		retMap.put(RESULT_KEY_OUTPUT, Arrays.asList(new String[]{output}));
		return retMap;
	}
}
