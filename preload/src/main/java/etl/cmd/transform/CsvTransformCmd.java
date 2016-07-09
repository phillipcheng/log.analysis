package etl.cmd.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import etl.engine.FileETLCmd;
import etl.engine.MRMode;
import etl.engine.ProcessMode;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class CsvTransformCmd extends FileETLCmd{
	public static final Logger logger = Logger.getLogger(CsvTransformCmd.class);
	private CsvTransformConf tfCfg;
	
	public CsvTransformCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		tfCfg = new CsvTransformConf(this.pc);
		this.setPm(ProcessMode.MRProcess);
		this.setMrMode(MRMode.line);
	}

	@Override
	public Map<String, List<String>> mrProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		Map<String, List<String>> retMap = new HashMap<String, List<String>>();
		if (tfCfg.isSkipHeader() && offset==0) {
			logger.info("skip header:" + row);
			return null;
		}

		String output="";
		tfCfg.clearMerger();//clear all the state from last line
		
		//get the list of items from the record
		List<String> items = new ArrayList<String>();
		StringTokenizer tn = new StringTokenizer(row, ",");
		while (tn.hasMoreTokens()){
			items.add(tn.nextToken());
		}
		
		if (tfCfg.isInputEndWithComma()){//remove the last empty item since row ends with comma
			items.remove(items.size()-1);
		}
		
		if (tfCfg.getRowValidation()!=null){
			Map<String, Object> map = new HashMap<String, Object>();
			map.put(CsvTransformConf.VAR_NAME_fields, items.toArray());
			boolean valid = (Boolean) ScriptEngineUtil.eval(tfCfg.getRowValidation(), VarType.BOOLEAN, map);
			if (!valid) {
				logger.info("invalid row:" + row);
				return null;
			}
		}
		
		//process the list of items
		int totalTokens = items.size();
		for (int tIdx=0; tIdx<totalTokens; tIdx++){
			String item = items.get(tIdx);
			//process remover, no output of this, continue processing
			ColUpdate updater = tfCfg.getUpdater(tIdx);
			if (updater!=null){
				item = updater.process(item);
			}
			
			//process merge
			ColMerger merger = tfCfg.getMerger(tIdx);
			if (merger!=null){
				if (merger.add(tIdx, item)){
					output+=merger.getValue();
					output+=",";
				}
				continue;
			}
			
			//process split
			ColSpliter spliter = tfCfg.getSpliter(tIdx);
			if (spliter!=null){
				String[] subitems = item.split(Pattern.quote(spliter.getSep()));
				for(int i=0; i<subitems.length; i++){
					output +=subitems[i];
					output +=",";
				}
				continue;
			}
			
			output+=item;
			output+=",";
			
		}
		if (isAddFileName()){
			output+=getAbbreFileName(((FileSplit) context.getInputSplit()).getPath().getName());
		}
		logger.info("output:" + output);
		retMap.put(RESULT_KEY_OUTPUT, Arrays.asList(new String[]{output}));
		return retMap;
	}
}
