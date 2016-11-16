package etl.cmd;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import etl.engine.ETLCmd;
import etl.engine.MRMode;

//key colon value format to csv
public class KcvToCsvCmd extends ETLCmd{
	private static final long serialVersionUID = 1L;
	
	public static final Logger logger = LogManager.getLogger(KcvToCsvCmd.class);
	
	//record format overall specification
	public static final String cfgkey_record_start="record.start";
		public static final String RECORD_SINGLELINE="^"; //single line
	public static final String cfgkey_record_vkexp="record.vkexp";
	public static final String cfgkey_record_fieldnum="record.fieldnum";
		public static final int RECORD_FIELDNUM_DEFAULT=-1; //extract all fields recognized

	//record format definition
	private String recordStart = RECORD_SINGLELINE;
	private Pattern recordStartPattern = null;
	private Pattern recordVKExp = null;
	private int recordFieldNum = RECORD_FIELDNUM_DEFAULT;
	
	public KcvToCsvCmd(){
		super();
	}
	
	public KcvToCsvCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		this.setMrMode(MRMode.file);
		String strVal = super.getCfgString(cfgkey_record_start, null);
		if (strVal!=null){
			this.recordStart = strVal;
			recordStartPattern = Pattern.compile(strVal);
		}

		strVal = super.getCfgString(cfgkey_record_vkexp, null);
		if (strVal!=null){
			this.recordVKExp = Pattern.compile(strVal);
		}
		
		strVal = super.getCfgString(cfgkey_record_fieldnum, null);
		if (strVal!=null){
			this.recordFieldNum = Integer.parseInt(strVal);
		}
	}

	//
	private String processRecord(String record, String filename){
		String output="";
		
		//get the list of items from the record
		List<String> items = new ArrayList<String>();
		StringTokenizer tn = new StringTokenizer(record, ":");
		int idx=0;
		int totalToken = tn.countTokens();
		while (tn.hasMoreTokens()){
			String vk = tn.nextToken(); //key, [value-key]+, value. ignore the 1st key, pass value-key pairs down
			if (idx>0){//2nd till size-1 are all value-key pairs
				if (idx<totalToken-1){
					if (recordFieldNum==-1 || idx<recordFieldNum){
						Matcher m = recordVKExp.matcher(vk);
						if (m.matches()){
							String val = m.group(1);
							String key = m.group(2);
							logger.debug(String.format("get value:%s, key:%s", val, key));
							items.add(val);
						}
					}
				}else{
					//last token is value
					items.add(vk);
				}
			}
			idx++;
		}
	
		//process the list of items
		int totalTokens = items.size();
		for (int tIdx=0; tIdx<totalTokens; tIdx++){
			String item = items.get(tIdx);
			if (tIdx<totalTokens-1){
				//omit last comma
				output+=item;
				output+=",";
			}
		}
		
		return output;
	}
	
	//fix file name
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context) {
		super.init();
		String filename = row;
		List<String> outputList = new ArrayList<String>();
		Path kcvFile = new Path(row);
		BufferedReader br = null;
		try {
			String line;
			br=new BufferedReader(new InputStreamReader(fs.open(kcvFile)));
			String record="";
			String lastRecord="";
			boolean found =false; //found a record
			while ((line = br.readLine()) != null) {
				if (RECORD_SINGLELINE.equals(recordStart)){
					record=line;
					lastRecord = record;
					found=true;
				}else{
					Matcher m = recordStartPattern.matcher(line);
					if (!m.matches()){
						record += System.lineSeparator() + line;
					}else{
						lastRecord = record;
						found = true;
						record = line;
					}
				}
				if (found && !"".equals(lastRecord)){
					outputList.add(processRecord(lastRecord, filename));
					lastRecord="";
					found=false;
				}
			}
			//last record
			if (!"".equals(lastRecord)){
				outputList.add(processRecord(lastRecord, filename));	
				lastRecord="";
				found=false;
			}
			
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (br!= null){
				try{
					br.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put(RESULT_KEY_OUTPUT_LINE, outputList);
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(outputList.size()+"");
		retMap.put(RESULT_KEY_LOG, logInfo);
		return retMap;
	}
	
	@Override
	public boolean hasReduce(){
		return false;
	}
}
