package log.analysis.preload;

public class ColAppender {
	public static final String COMMAND="append.idx";
	
	private int idx;
	private int afterIdx; //append after this reverse index (for example, afterIdx=2 means append at the 2nd character before tail)
	private String suffix;
	
	
	public ColAppender(int idx, int afterIdx, String suffix){
		this.idx = idx;
		this.setAfterIdx(afterIdx);
		this.setSuffix(suffix);
	}
	
	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	public int getAfterIdx() {
		return afterIdx;
	}

	public void setAfterIdx(int afterIdx) {
		this.afterIdx = afterIdx;
	}

}
