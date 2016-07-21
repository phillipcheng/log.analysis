package etl.cmd.transform;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.engine.FileETLCmd;
import etl.engine.MRMode;
import etl.engine.ProcessMode;

//key colon value format to csv
public class KcvToCsvCmd extends FileETLCmd{
	public static final Logger logger = Logger.getLogger(KcvToCsvCmd.class);
	public static final String cfgkey_kcv_folder="kcv.folder";
	public static final String cfgkey_use_wfid="use.wfid";
	
	//record format overall specification
	public static final String RECORD_START="record.start";
		public static final String RECORD_SINGLELINE="^"; //single line

	public static final String RECORD_KCV_VK_REGEXP="record.vkexp";
		
	public static final String RECORD_FIELDNUM="record.fieldnum";
		public static final int RECORD_FIELDNUM_DEFAULT=-1; //extract all fields recognized

	//record format definition
	private String recordStart = RECORD_SINGLELINE;
	private Pattern recordStartPattern = null;
	private Pattern recordVKExp = null;
	private int recordFieldNum = RECORD_FIELDNUM_DEFAULT;
	private String kcvFolder;
	private boolean useWfid;
	
	public KcvToCsvCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		kcvFolder = pc.getString(cfgkey_kcv_folder);
		useWfid = pc.getBoolean(cfgkey_use_wfid, false);
		String strVal = pc.getString(RECORD_START);
		if (strVal!=null){
			this.recordStart = strVal;
			recordStartPattern = Pattern.compile(strVal);
		}

		strVal = pc.getString(RECORD_KCV_VK_REGEXP);
		if (strVal!=null){
			this.recordVKExp = Pattern.compile(strVal);
		}
		
		strVal = pc.getString(RECORD_FIELDNUM);
		if (strVal!=null){
			this.recordFieldNum = Integer.parseInt(strVal);
		}
		this.setMrMode(MRMode.file);
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
		if (isAddFileName()){
			output+=",";
			output+=getAbbreFileName(filename);
		}
		return output;
	}
	
	//fix file name
	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		String filename = row;
		List<String> outputList = new ArrayList<String>();
		Path kcvFile = null;
		if (useWfid){
			kcvFile = new Path(kcvFolder + "/" + wfid + "/" + row);
		}else{
			kcvFile = new Path(kcvFolder + "/" + row);
		}
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
		retMap.put(RESULT_KEY_OUTPUT, outputList);
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(outputList.size()+"");
		retMap.put(RESULT_KEY_LOG, logInfo);
		return retMap;
	}
}
