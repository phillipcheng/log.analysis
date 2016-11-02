package etl.cmd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.cmd.transform.ColOp;
import etl.engine.ETLCmd;
import etl.engine.MRMode;
import etl.engine.OutputType;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;
import scala.Tuple2;

public class CsvCountCmd extends SchemaFileETLCmd{

	private static final long serialVersionUID = -5841598886000954968L;

	public static final Logger logger = LogManager.getLogger(CsvCountCmd.class);
	
	//cfgkey
	public static final String cfgkey_split_dimension_exp="split.dimension.exp";
	public static final String cfgkey_group_dimensions_exp="group.dimensions.exp";
	public static final String cfgkey_skip_header="skip.header";
	public static final String cfgkey_row_validation="row.validation";
	public static final String cfgkey_input_endwithcomma="input.endwithcomma";
	
	private String splitDimensionExp;
	private CompiledScript splitDimensionCS=null;
	private String groupDimensionExp;
	private CompiledScript groupDimensionCS=null;
	private boolean skipHeader=false;
	private String rowValidation;
	private boolean inputEndWithComma=false;	
	
	public CsvCountCmd(){
		super();
	}
	
	public CsvCountCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvCountCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		this.setMrMode(MRMode.line);
		splitDimensionExp =super.getCfgString(cfgkey_split_dimension_exp, null);
		groupDimensionExp =super.getCfgString(cfgkey_group_dimensions_exp, null);
		skipHeader =super.getCfgBoolean(cfgkey_skip_header, false);
		rowValidation = super.getCfgString(cfgkey_row_validation, null);
		inputEndWithComma = super.getCfgBoolean(cfgkey_input_endwithcomma, false);
		
		if(splitDimensionExp!=null){
			splitDimensionCS=ScriptEngineUtil.compileScript(splitDimensionExp);
		}
		
		if(groupDimensionExp!=null){
			groupDimensionCS=ScriptEngineUtil.compileScript(groupDimensionExp);
		}
		 
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) {
		Map<String, Object> retMap = new HashMap<String, Object>();
		//process skip header
		if (skipHeader && offset==0) {
			logger.info("skip header:" + row);
			return null;
		}
		
		//get all fields
		List<String> items = new ArrayList<String>();
		items.addAll(Arrays.asList(row.split(",", -1)));
		
		//process input ends with comma
		if (inputEndWithComma) {// remove the last empty item since row ends with comma
			items.remove(items.size() - 1);
		}		
		getSystemVariables().put(ColOp.VAR_NAME_FIELDS, items.toArray(new String[0]));
		
		//process row validation
		if (rowValidation != null) {
			boolean valid = (Boolean) ScriptEngineUtil.eval(rowValidation, VarType.BOOLEAN, super.getSystemVariables());
			if (!valid) {
				logger.info("invalid row:" + row);
				return null;
			}
		}
		
		//get dims
		String groupDimsValue=null;
		if(groupDimensionExp!=null){
			groupDimsValue=(String) ScriptEngineUtil.eval(this.groupDimensionCS, super.getSystemVariables());
			if(groupDimsValue==null){
				logger.info("No dims:" + row);
				return null;
			}
		}
		
		String[] splitDimsValueArray=null;
		if(splitDimensionExp!=null){			
			splitDimsValueArray=(String[]) ScriptEngineUtil.evalObject(this.splitDimensionCS, super.getSystemVariables());
			if(splitDimsValueArray==null || splitDimsValueArray.length==0){
				logger.info("No splited dims:" + row);
				return null;
			}
		}
		
		//calculate output
		if(splitDimensionExp!=null){
			List<Tuple2<String, String>> output=new ArrayList<Tuple2<String, String>>();
			for(String splitedDimValue:splitDimsValueArray){
				if(splitedDimValue==null){
					logger.info("Splited dims cannot be null:" + row);
					return null;
				}
				if(groupDimensionExp!=null){
					output.add(new Tuple2<String, String>(splitedDimValue+","+groupDimsValue, "1"));
				}else{
					output.add(new Tuple2<String, String>(splitedDimValue, "1"));
				}
				
			}
			retMap.put(RESULT_KEY_OUTPUT_TUPLE2, output);
		}else if(groupDimensionExp!=null){
			Tuple2<String, String> ret = new Tuple2<String, String>(groupDimsValue,"1");
			retMap.put(RESULT_KEY_OUTPUT_TUPLE2, Arrays.asList(ret));
		}else{
			logger.info("One of split.dimension.exp or group.dimensions.exp is required");
			return null;
		}
		return retMap;
	}
	
	
	@Override
	public List<String[]> reduceProcess(Text key, Iterable<Text> values){
		List<String[]> ret = new ArrayList<String[]>();	
		Iterator<Text> it = values.iterator();
		long sum=0;
		while (it.hasNext()){
			long value=Long.parseLong(it.next().toString());
			sum=sum+value;
		}
		if (super.getOutputType()==OutputType.multiple){
			ret.add(new String[]{key.toString(), String.valueOf(sum), key.toString()});
		}else{
			ret.add(new String[]{key.toString(), String.valueOf(sum), ETLCmd.SINGLE_TABLE});
		}
		return ret;
	}
}
