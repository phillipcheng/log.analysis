package etl.cmd.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class PreloadConf {
	public static final Logger logger = Logger.getLogger(PreloadConf.class);
	
	//record format overall specification
	public static final String RECORD_START="record.start";
		public static final String RECORD_SINGLELINE="^"; //single line

	public static final String RECORD_FORMAT="record.format";
		public static final String RECORD_FORMAT_CSV="csv";
		public static final String RECORD_FORMAT_KCV="kcv"; //key colon value
	
	public static final String RECORD_KCV_VK_REGEXP="record.vkexp";
		
	public static final String RECORD_FIELDNUM="record.fieldnum";
		public static final int RECORD_FIELDNUM_DEFAULT=-1; //extract all fields recognized
		
	public static final String RECORD_KEEP="record.keep";

	public static final String NO_EVENT_OUTPUT="nothing.evt";
	//record wise preprocessing	
	
	//record type specification
	public static final String EVT_IDX="event.idx";
	public static final String EVT_TYPE_KEY="event.types";
	
	//main message specification
	public static final String MESSAGE_IDX="message.idx";
	public static final String MESSAGE_FIELDS="message.fields";
	public static final String REGEXP_KEY="regexp";
	public static final String ATTR_KEY="attr";
	public static final String DEFAULT_EVENT_TYPE="default";
	
	//TODO remove these special treatment to plugin
	public static final String SPECIAL_TREAT_E164="E164";
	public static final String SPECIAL_TREAT_GTAddr="GTAddr";
	public static final String COUNTRY_CODE_LIST="countrycode.list";
	public static final String NANPA_LIST="nanpa.list";
	
	//record format definition
	private String recordStart = RECORD_SINGLELINE;
	private Pattern recordStartPattern = null;
	private String recordFormat=RECORD_FORMAT_CSV;
	private Pattern recordVKExp = null;
	private int recordFieldNum = RECORD_FIELDNUM_DEFAULT;
	private boolean recordKeep = false;
	
	//event
	private int eventIdx;
	private Map<String, Pattern> evtPtnMap = new HashMap<String, Pattern>();//event pattern map
	//message
	private int msgIdx;
	private String[] msgFields;
	private Map<String, String[]> msgAttrMap = new HashMap<String, String[]>();//attr map
	
	//unidentified evt types
	private Set<String> missedEvtType = new HashSet<String>();
	
	//plugin
	private CountryCode ccMap = new CountryCode();
	private NanpaCode nanpaMap = new NanpaCode();
	
	public PreloadConf(String fileName){
		PropertiesConfiguration pc = null;
		try{
			pc =new PropertiesConfiguration(fileName);
		}catch(Exception e){
			logger.error("", e);
		}
		
		//record overall configuration
		String strVal = pc.getString(PreloadConf.RECORD_START);
		if (strVal!=null){
			this.recordStart = strVal;
			this.setRecordStartPattern(Pattern.compile(strVal));
		}
		
		strVal = pc.getString(PreloadConf.RECORD_FORMAT);
		if (strVal==null){
			this.recordFormat = PreloadConf.RECORD_FORMAT_CSV;
		}else if (PreloadConf.RECORD_FORMAT_CSV.equals(strVal)){
			this.recordFormat = PreloadConf.RECORD_FORMAT_CSV;
		}else if (PreloadConf.RECORD_FORMAT_KCV.equals(strVal)){
			this.recordFormat = PreloadConf.RECORD_FORMAT_KCV;
		}else{
			logger.error("unsupported record format type:" + strVal);
		}
		strVal = pc.getString(PreloadConf.RECORD_KCV_VK_REGEXP);
		if (strVal!=null){
			this.recordVKExp = Pattern.compile(strVal);
		}
		
		strVal = pc.getString(PreloadConf.RECORD_FIELDNUM);
		if (strVal!=null){
			this.recordFieldNum = Integer.parseInt(strVal);
		}
		
		strVal = pc.getString(PreloadConf.RECORD_KEEP);
		if (strVal!=null){
			this.recordKeep = Boolean.parseBoolean(strVal);
		}
		
		//evt conf
		try{
			eventIdx = pc.getInt(EVT_IDX);
		}catch(NoSuchElementException nsee){
			eventIdx=-1;
		}
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
		
		//message conf
		try{
			msgIdx = pc.getInt(MESSAGE_IDX);
		}catch(NoSuchElementException nsee){
			msgIdx=-1;
		}
		msgFields = pc.getStringArray(MESSAGE_FIELDS);
		
		//plugin
		String ccFile = pc.getString(COUNTRY_CODE_LIST);
		if (ccFile!=null)
			ccMap.init(ccFile);
		
		String nanpaFile = pc.getString(NANPA_LIST);
		if (nanpaFile!=null)
			nanpaMap.init(nanpaFile);
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
					if (SPECIAL_TREAT_E164.equals(fieldName)){
						//append country code
						output+=",";
						String cc = ccMap.getCode(value);
						if (cc!=null){
							output+=cc;
						}else{
							logger.error(String.format("country code not found for %s", value));
						}
						output+=",";
						//append nanpa code
						if (cc!=null && "1".equals(cc)){
							value = value.substring(cc.length());
							if (value.length()>6){
								String str1 = value.substring(0, 3);
								String str2 = value.substring(3,6);
								String nanpaCode = str1 + "-" + str2;
								if (nanpaMap.hasNanpa(nanpaCode)){
									output+=nanpaCode;
								}else{
									logger.error("nanpaCode not found for:" + value);
								}
							}else{
								logger.error("nanpaCode not found for:" + value);
							}
						}
					}
					if (SPECIAL_TREAT_GTAddr.equals(fieldName)){
						//append country code
						output+=",";
						String cc = ccMap.getCode(value);
						if (cc!=null){
							output+=cc;
						}else{
							logger.error(String.format("country code not found for %s", value));
						}
					}
				}else{
					//value does not have the field, still needs to add padding for special treated fields
					if (SPECIAL_TREAT_E164.equals(fieldName)){
						output+=",,";
					}
					if (SPECIAL_TREAT_GTAddr.equals(fieldName)){
						output+=",";
					}
				}
				output+=",";
			}
		}
		return output;
	}

	public int getEventIdx() {
		return eventIdx;
	}

	public int getMsgIdx() {
		return msgIdx;
	}

	public String getRecordFormat() {
		return recordFormat;
	}

	public void setRecordFormat(String recordFormat) {
		this.recordFormat = recordFormat;
	}

	public String getRecordStart() {
		return recordStart;
	}

	public void setRecordStart(String recordStart) {
		this.recordStart = recordStart;
	}

	public int getRecordFieldNum() {
		return recordFieldNum;
	}

	public void setRecordFieldNum(int recordFieldNum) {
		this.recordFieldNum = recordFieldNum;
	}

	public boolean isRecordKeep() {
		return recordKeep;
	}

	public void setRecordKeep(boolean recordKeep) {
		this.recordKeep = recordKeep;
	}

	public Pattern getRecordStartPattern() {
		return recordStartPattern;
	}

	public void setRecordStartPattern(Pattern recordStartPattern) {
		this.recordStartPattern = recordStartPattern;
	}

	public Pattern getRecordVKExp() {
		return recordVKExp;
	}

	public void setRecordVKExp(Pattern recordVKExp) {
		this.recordVKExp = recordVKExp;
	}
}
