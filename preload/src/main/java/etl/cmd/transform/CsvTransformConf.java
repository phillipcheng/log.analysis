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

	//operations
	private List<ColMerger> mergers = new ArrayList<ColMerger>(); 
	private Map<Integer, ColSpliter> splits = new TreeMap<Integer, ColSpliter>();
	private Map<Integer, ColRemover> removers = new TreeMap<Integer, ColRemover>();
	private Map<Integer, ColAppender> appenders = new TreeMap<Integer, ColAppender>();
	private Map<Integer, ColPrepender> prependers = new TreeMap<Integer, ColPrepender>();
	
	public ColRemover getRemover(int idx){
		return removers.get(idx);
	}
	
	public ColSpliter getSpliter(int idx){
		return splits.get(idx);
	}
	
	public ColAppender getAppender(int idx){
		return appenders.get(idx);
	}
	
	public ColPrepender getPrepender(int idx){
		return prependers.get(idx);
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
	
	public CsvTransformConf(PropertiesConfiguration pc){
		
		//record preprocessing
		//remove
		String[] removeIdxStrs = pc.getStringArray(ColRemover.COMMAND);
		if (removeIdxStrs!=null){
			for (String removeIdxStr:removeIdxStrs){
				StringTokenizer st = new StringTokenizer(removeIdxStr,":");
				int i=0;
				String rm="";
				int rmIdx=0;
				while(st.hasMoreTokens()){
					String token = st.nextToken();
					if (i==0){
						rmIdx = Integer.parseInt(token);
					}else if (i==1){
						rm = token;
					}
					i++;
				}
				ColRemover remover = new ColRemover(rmIdx, rm);
				removers.put(rmIdx, remover);
			}
		}
		
		//merge
		String[] mergeIdxStrs = pc.getStringArray(ColMerger.COMMAND);
		if (mergeIdxStrs!=null){
			for (String mergeIdxStr:mergeIdxStrs){
				StringTokenizer st = new StringTokenizer(mergeIdxStr,":");
				int i=0;
				int start=0,end=0;
				String joiner="-";
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
		
		//append
		String[] appendIdxStrs = pc.getStringArray(ColAppender.COMMAND);
		if (appendIdxStrs!=null){
			for (String appendIdxStr:appendIdxStrs){
				StringTokenizer st = new StringTokenizer(appendIdxStr,":");
				int i=0;
				String suffix="";
				int appendIdx=0;
				int afterIdx=0;
				while(st.hasMoreTokens()){
					String token = st.nextToken();
					if (i==0){
						appendIdx = Integer.parseInt(token);
					}else if (i==1){
						afterIdx = Integer.parseInt(token);
					}else if (i==2){
						suffix = token;
					}
					i++;
				}
				ColAppender appender = new ColAppender(appendIdx, afterIdx, suffix);
				appenders.put(appendIdx, appender);
			}
		}
		
		//prepend
		String[] prependIdxStrs = pc.getStringArray(ColPrepender.COMMAND);
		if (prependIdxStrs!=null){
			for (String prependIdxStr:prependIdxStrs){
				StringTokenizer st = new StringTokenizer(prependIdxStr,":");
				int i=0;
				String prefix="";
				int beforeIdx=0;
				int idx=0;
				while(st.hasMoreTokens()){
					String token = st.nextToken();
					if (i==0){
						idx = Integer.parseInt(token);
					}else if (i==1){
						beforeIdx = Integer.parseInt(token);
					}else if (i==2){
						prefix = token;
					}
					i++;
				}
				ColPrepender prepender = new ColPrepender(idx, beforeIdx, prefix);
				prependers.put(idx, prepender);
			}
		}
	}
}
