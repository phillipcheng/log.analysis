package etl.cmd.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class CsvTransformConf {
	public static final Logger logger = Logger.getLogger(CsvTransformConf.class);

	public static final String cfgkey_skip_header="skip.header";
	public static final String cfgkey_row_validation="row.validation";
	public static final String cfgkey_input_endwithcomma="input.endwithcomma";
	
	public static final String VAR_NAME_fields="fields";
	
	//operations
	private List<ColMerger> mergers = new ArrayList<ColMerger>(); 
	private Map<Integer, ColSpliter> splits = new TreeMap<Integer, ColSpliter>();
	private Map<Integer, ColUpdate> updates = new TreeMap<Integer, ColUpdate>();
	//
	private boolean skipHeader=false;
	private String rowValidation;
	private boolean inputEndWithComma=false;
	
	public CsvTransformConf(PropertiesConfiguration pc){
		setSkipHeader(pc.getBoolean(cfgkey_skip_header, false));
		setRowValidation(pc.getString(cfgkey_row_validation));
		setInputEndWithComma(pc.getBoolean(cfgkey_input_endwithcomma, false));
		
		//record preprocessing
		//single column updates
		String[] updateIdxStrs = pc.getStringArray(ColUpdate.COMMAND);
		if (updateIdxStrs!=null){
			for (String updateIdxStr:updateIdxStrs){
				StringTokenizer st = new StringTokenizer(updateIdxStr,":");
				int i=0;
				String exp="";
				int colIdx=0;
				while(st.hasMoreTokens()){
					String token = st.nextToken();
					if (i==0){
						colIdx = Integer.parseInt(token);
					}else if (i==1){
						exp = token;
					}
					i++;
				}
				updates.put(colIdx, new ColUpdate(colIdx, exp));
			}
		}
		
		//merge
		String[] mergeIdxStrs = pc.getStringArray(ColMerger.COMMAND);
		if (mergeIdxStrs!=null){
			for (String mergeIdxStr:mergeIdxStrs){
				StringTokenizer st = new StringTokenizer(mergeIdxStr,":");
				int i=0;
				int start=0,end=0;
				String joiner="";
				while(st.hasMoreTokens()){
					String token = st.nextToken();
					if (i==0){
						start = Integer.parseInt(token);
					}else if (i==1){
						end = Integer.parseInt(token);
					}else if (i==2){
						joiner = token;
					}
					i++;
				}
				int size = end-start+1;
				int[] midx = new int[size];
				for (i=0; i<size; i++){
					midx[i]=start+i;
				}
				ColMerger merger = new ColMerger(midx, joiner);
				mergers.add(merger);
			}
		}
		
		//split
		String[] splitIdxStrs = pc.getStringArray(ColSpliter.COMMAND);
		if (splitIdxStrs!=null){
			for (String splitIdxStr:splitIdxStrs){
				StringTokenizer st = new StringTokenizer(splitIdxStr,":");
				int i=0;
				String sep=".";
				int splitIdx=0;
				while(st.hasMoreTokens()){
					String token = st.nextToken();
					if (i==0){
						splitIdx = Integer.parseInt(token);
					}else if (i==1){
						sep = token;
					}
					i++;
				}
				ColSpliter spliter = new ColSpliter(splitIdx, sep);
				splits.put(splitIdx, spliter);
			}
		}
	}
	
	public ColUpdate getUpdater(int idx){
		return updates.get(idx);
	}
	
	public ColSpliter getSpliter(int idx){
		return splits.get(idx);
	}
	
	public void clearMerger(){
		for (int i=0; i<mergers.size(); i++){
			ColMerger m = mergers.get(i);
			m.reinit();
		}
	}
	
	public ColMerger getMerger(int idx){
		for (int i=0; i<mergers.size(); i++){
			ColMerger m = mergers.get(i);
			if (m.contains(idx)){
				return m;
			}
		}
		return null;
	}

	public boolean isSkipHeader() {
		return skipHeader;
	}

	public void setSkipHeader(boolean skipHeader) {
		this.skipHeader = skipHeader;
	}

	public String getRowValidation() {
		return rowValidation;
	}

	public void setRowValidation(String rowValidation) {
		this.rowValidation = rowValidation;
	}

	public boolean isInputEndWithComma() {
		return inputEndWithComma;
	}

	public void setInputEndWithComma(boolean inputEndWithComma) {
		this.inputEndWithComma = inputEndWithComma;
	}
}
