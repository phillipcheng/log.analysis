package etl.cmd;

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

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import etl.engine.ETLCmd;
import etl.engine.MRMode;

public class EvtBasedMsgParseCmd extends ETLCmd{
	public static final Logger logger = LogManager.getLogger(EvtBasedMsgParseCmd.class);
	//record type specification
	public static final String cfgkey_evt_idx="event.idx";
	public static final String cfgkey_evt_types="event.types";
	
	//main message specification
	public static final String cfgkey_msg_idx="message.idx";
	public static final String cfgkey_msg_fields="message.fields";
	public static final String REGEXP_KEY="regexp";
	public static final String ATTR_KEY="attr";
	public static final String DEFAULT_EVENT_TYPE="default";

	//event
	private int eventIdx=-1;
	private Map<String, Pattern> evtPtnMap = new HashMap<String, Pattern>();//event pattern map
	//message
	private int msgIdx=-1;
	private String[] msgFields;
	private Map<String, String[]> msgAttrMap = new HashMap<String, String[]>();//attr map
	private Set<String> missedEvtType = new HashSet<String>();
	
	public EvtBasedMsgParseCmd(){
		super();
	}
	
	public EvtBasedMsgParseCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public EvtBasedMsgParseCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		this.setMrMode(MRMode.line);
		eventIdx = super.getCfgInt(cfgkey_evt_idx, -1);
		String[] evtTypes = super.getCfgStringArray(cfgkey_evt_types);
		for (String et:evtTypes){
			String regexp = super.getCfgString(et + "." + REGEXP_KEY, null);
			if (regexp!=null){
				Pattern p = Pattern.compile("^" + regexp + "$");
				evtPtnMap.put(et, p);
				String[] attrs = super.getCfgStringArray(et + "." + ATTR_KEY);
				if (attrs!=null){
					msgAttrMap.put(et, attrs);
				}
			}
		}
		msgIdx = super.getCfgInt(cfgkey_msg_idx, -1);
		msgFields = super.getCfgStringArray(cfgkey_msg_fields);
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
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) {
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
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put(RESULT_KEY_OUTPUT_LINE, Arrays.asList(new String[]{output}));
		return retMap;
	}
	
	@Override
	public boolean hasReduce(){
		return false;
	}
}
