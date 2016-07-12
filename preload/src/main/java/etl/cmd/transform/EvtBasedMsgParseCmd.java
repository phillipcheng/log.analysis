package etl.cmd.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;
import etl.engine.MRMode;
import etl.engine.ProcessMode;

public class EvtBasedMsgParseCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(EvtBasedMsgParseCmd.class);
	//record type specification
	public static final String EVT_IDX="event.idx";
	public static final String EVT_TYPE_KEY="event.types";
	
	//main message specification
	public static final String MESSAGE_IDX="message.idx";
	public static final String MESSAGE_FIELDS="message.fields";
	public static final String REGEXP_KEY="regexp";
	public static final String ATTR_KEY="attr";
	public static final String DEFAULT_EVENT_TYPE="default";

	public static final String EM_PRE="*";
	public static final String EM_POST=" IMSI:";
	public static final String EM_POST2=":";

	//event
	private int eventIdx=-1;
	private Map<String, Pattern> evtPtnMap = new HashMap<String, Pattern>();//event pattern map
	//message
	private int msgIdx=-1;
	private String[] msgFields;
	private Map<String, String[]> msgAttrMap = new HashMap<String, String[]>();//attr map
	private Set<String> missedEvtType = new HashSet<String>();
	
	public EvtBasedMsgParseCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		eventIdx = pc.getInt(EVT_IDX, -1);
		String[] evtTypes = pc.getStringArray(EVT_TYPE_KEY);
		for (String et:evtTypes){
			String regexp = pc.getString(et + "." + REGEXP_KEY);
			Pattern p = Pattern.compile("^" + regexp + "$");
			evtPtnMap.put(et, p);
			String[] attrs = pc.getStringArray(et + "." + ATTR_KEY);
			if (attrs!=null){
				msgAttrMap.put(et, attrs);
			}
		}
		msgIdx = pc.getInt(MESSAGE_IDX, -1);
		msgFields = pc.getStringArray(MESSAGE_FIELDS);
		this.setMrMode(MRMode.line);
	}
	
	public String getOutputValues(String evtType, String input){
		Pattern p = null;
		String[] attrs = null;
		if (evtPtnMap.containsKey(evtType)){
			p = evtPtnMap.get(evtType);
			attrs = msgAttrMap.get(evtType);
		}else{
			p = evtPtnMap.get(DEFAULT_EVENT_TYPE);
			attrs = msgAttrMap.get(DEFAULT_EVENT_TYPE);
			if (!missedEvtType.contains(evtType)){
				missedEvtType.add(evtType);
				logger.warn("evtType not found using default:" + evtType);
			}
		}
		Map<String, String> values = new HashMap<String, String>();
		Matcher m = p.matcher(input);
		if (m.find()){
			for (int i=1; i<=m.groupCount(); i++){
				String key = attrs[i-1];
				String val = m.group(i);
				values.put(key, val);
			}
		}
		String output="";
		if (msgFields!=null){
			for (String fieldName: msgFields){
				if (values.containsKey(fieldName)){
					String value = values.get(fieldName);
					output+=value;	
				}
				output+=",";
			}
		}
		return output;
	}

	@Override
	public Map<String,List<String>> mrProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		String output="";
		String evtType="";
		
		//get the list of items from the record
		List<String> items = new ArrayList<String>();
		StringTokenizer tn = new StringTokenizer(row, ",");
		while (tn.hasMoreTokens()){
			items.add(tn.nextToken());
		}
		//process the list of items
		int totalTokens = items.size();
		for (int tIdx=0; tIdx<totalTokens; tIdx++){
			String item = items.get(tIdx);
			
			//event idx
			if (tIdx == eventIdx){
				evtType = item;
				output+=item;
				output+=",";
				continue;
			}
			
			//msg idx
			if (tIdx == msgIdx){
				output+=item;
				output+=",";
				output+=getOutputValues(evtType, item);
				continue;
			}
			
			if (tIdx<totalTokens-1){
				//omit last comma
				output+=item;
				output+=",";
			}
		}
		logger.info("output:" + output);
		Map<String,List<String>> retMap = new HashMap<String, List<String>>();
		retMap.put(RESULT_KEY_OUTPUT, Arrays.asList(new String[]{output}));
		return retMap;
	}
}
