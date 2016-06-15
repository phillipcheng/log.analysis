package etl.cmd.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import etl.engine.FileETLCmd;

public class CsvTransformCmd extends FileETLCmd{

	private CsvTransformConf plc;
	
	public CsvTransformCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		plc = new CsvTransformConf(this.pc);
	}

	@Override
	public List<String> process(String record, Mapper<Object, Text, Text, NullWritable>.Context context) {
		String output="";
		plc.clearMerger();//clear all the state from last line
		
		//get the list of items from the record
		List<String> items = new ArrayList<String>();
		StringTokenizer tn = new StringTokenizer(record, ",");
		while (tn.hasMoreTokens()){
			items.add(tn.nextToken());
		}
		
		
		//process the list of items
		int totalTokens = items.size();
		for (int tIdx=0; tIdx<totalTokens; tIdx++){
			String item = items.get(tIdx);
			//process remover, no output of this, continue processing
			ColRemover remover = plc.getRemover(tIdx);
			if (remover!=null){
				item = item.replace(remover.getRm(), "");
			}
			
			ColAppender appender = plc.getAppender(tIdx);
			if (appender!=null){
				StringBuilder sb = new StringBuilder(item);
				sb = sb.insert(item.length()-appender.getAfterIdx(), appender.getSuffix());
				item = sb.toString();
			}
			
			ColPrepender prepender = plc.getPrepender(tIdx);
			if (prepender!=null){
				StringBuilder sb = new StringBuilder(item);
				sb = sb.insert(prepender.getBeforeIdx(), prepender.getPrefix());
				item = sb.toString();
			}
			
			//process merge
			ColMerger merger = plc.getMerger(tIdx);
			if (merger!=null){
				if (merger.add(tIdx, item)){
					output+=merger.getValue();
					output+=",";
				}
				continue;
			}
			
			//process split
			ColSpliter spliter = plc.getSpliter(tIdx);
			if (spliter!=null){
				String[] subitems = item.split(Pattern.quote(spliter.getSep()));
				for(int i=0; i<subitems.length; i++){
					output +=subitems[i];
					output +=",";
				}
				continue;
			}
			
			if (tIdx<totalTokens-1){
				//omit last comma
				output+=item;
				output+=",";
			}
		}
		if (isAddFileName()){
			output+=",";
			output+=getAbbreFileName(((FileSplit) context.getInputSplit()).getPath().getName());
		}
		return Arrays.asList(new String[]{output});
	}
}
