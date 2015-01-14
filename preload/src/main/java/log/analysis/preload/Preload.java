package log.analysis.preload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.cld.util.StringUtil;

public class Preload {
	public static final Logger logger = Logger.getLogger(Preload.class);
	public static final String EM_PRE="*";
	public static final String EM_POST=" IMSI:";
	public static final String EM_POST2=":";
	
	private PreloadConf plc;
	private Map<String, String> evtDefMap= new TreeMap<String, String>();
	
	private String getErrorTypeMsg(String item){
		//try to extract the error type message
		//From * to " IMSI:"
		String errorTypeMsg = null;
		if (item.contains(EM_PRE) && item.contains(EM_POST)){
			errorTypeMsg = StringUtil.getStringBetweenFirstPreFirstPost(item, EM_PRE, EM_POST);
		}else if (item.contains(EM_PRE) && item.contains(EM_POST2)){
			errorTypeMsg = StringUtil.getStringBetweenFirstPreFirstPost(item, EM_PRE, EM_POST2);
		}else if (item.contains(EM_POST2)){
			errorTypeMsg = StringUtil.getStringBetweenFirstPreFirstPost(item, null, EM_POST2);
		}else{
			logger.warn("unregnized error message:" + item);
		}
		return errorTypeMsg;
	}
	
	//
	private String processRecord(String record){
		String output="";
		String evtType="";
		plc.clearMerger();//clear all the state from last line
		
		//get the list of items from the record
		List<String> items = new ArrayList<String>();
		if (PreloadConf.RECORD_FORMAT_CSV.equals(plc.getRecordFormat())){
			StringTokenizer tn = new StringTokenizer(record, ",");
			while (tn.hasMoreTokens()){
				items.add(tn.nextToken());
			}
		}else if (PreloadConf.RECORD_FORMAT_KCV.equals(plc.getRecordFormat())){
			StringTokenizer tn = new StringTokenizer(record, ":");
			int idx=0;
			int totalToken = tn.countTokens();
			while (tn.hasMoreTokens()){
				String vk = tn.nextToken(); //key, [value-key]+, value. ignore the 1st key, pass value-key pairs down
				if (idx>0){//2nd till size-1 are all value-key pairs
					if (idx<totalToken-1){
						if (plc.getRecordFieldNum()==-1 || idx<plc.getRecordFieldNum()){
							Matcher m = plc.getRecordVKExp().matcher(vk);
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
			
			//event idx
			if (tIdx == plc.getEventIdx()){
				evtType = item;
				output+=item;
				output+=",";
				continue;
			}
			
			//msg idx
			if (tIdx == plc.getMsgIdx()){
				output+=item;
				String errorTypeMsg = getErrorTypeMsg(item);
				//
				if (errorTypeMsg!=null){
					if (!evtDefMap.containsKey(evtType)){
						evtDefMap.put(evtType, errorTypeMsg);
					}else{
						String oldMsg = evtDefMap.get(evtType);
						if (!oldMsg.equals(errorTypeMsg)){
							logger.warn(String.format("error type msg for %s differs, old:%s, new:%s", 
									evtType, oldMsg, errorTypeMsg));
						}
					}
				}
				output+=",";
				output+=plc.getOutputValues(evtType, item);
				continue;
			}
			
			if (tIdx<totalTokens-1){
				//omit last comma
				output+=item;
				output+=",";
			}
		}
		return output;
	}
	
	public void processFile(String prop, String[] infile, String outfile, String outputEvtTypeFile){
		plc = new PreloadConf(prop);
		BufferedReader br = null;
		PrintWriter fw = null;
		PrintWriter etfw = null;
		try {
			fw = new PrintWriter(new FileWriter(outfile));
			String line;
			for (int i=0; i<infile.length; i++){
				br = new BufferedReader(new FileReader(infile[i]));
				String record="";
				String lastRecord="";
				boolean found =false; //found a record
				while ((line = br.readLine()) != null) {
					if (PreloadConf.RECORD_SINGLELINE.equals(plc.getRecordStart())){
						record=line;
						lastRecord = record;
						found=true;
						record = line;
					}else{
						Matcher m = plc.getRecordStartPattern().matcher(line);
						if (!m.matches()){
							record += System.lineSeparator() + line;
						}else{
							lastRecord = record;
							found = true;
							record = line;
						}
					}
					if (found && !"".equals(lastRecord)){
						fw.println(processRecord(lastRecord));
						lastRecord="";
						found=false;
					}
				}
				//last record
				if (!"".equals(lastRecord)){
					fw.println(processRecord(lastRecord));	
					lastRecord="";
					found=false;
				}
			}
		
			if (outputEvtTypeFile!=null && !PreloadConf.NO_EVENT_OUTPUT.equals(outputEvtTypeFile)){
				//open this in append mode
				etfw = new PrintWriter(new FileWriter(outputEvtTypeFile));
				Iterator<String> its = evtDefMap.keySet().iterator();
				while (its.hasNext()){
					String key = its.next();
					String msg = evtDefMap.get(key);
					etfw.println(String.format("%s,%s", key, msg));
				}
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
			if (fw!= null){
				try{
					fw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
			if (etfw!= null){
				try{
					etfw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public Preload(){
	}
	
	public static void main(String[] args){
		Preload epl = new Preload();
		if (args.length!=4){
			System.out.print("usage: EmsPreload preload.properties outputFile outputEvtTypeFile inputFile ...");
		}else{
			String[] inputFiles = new String[args.length-3];
			for (int i=3; i<args.length; i++){
				inputFiles[i-3]=args[i];
			}
			epl.processFile(args[0], inputFiles, args[1], args[2]);
		}
	}
}
