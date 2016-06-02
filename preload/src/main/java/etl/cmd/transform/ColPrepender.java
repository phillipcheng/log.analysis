package etl.cmd.transform;

public class ColPrepender {
	public static final String COMMAND="prepend.idx";
	
	private int idx;
	private int beforeIdx; //prepend before this idx
	private String prefix;
	
	
	public ColPrepender(int idx, int beforeIdx, String prefix){
		this.idx = idx;
		this.setBeforeIdx(beforeIdx);
		this.setPrefix(prefix);
	}
	
	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public int getBeforeIdx() {
		return beforeIdx;
	}

	public void setBeforeIdx(int beforeIdx) {
		this.beforeIdx = beforeIdx;
	}

}
