package log.analysis.preload;

public class Spliter {
	private int idx;
	private String sep;
	
	public Spliter(int idx, String sep){
		this.idx = idx;
		this.sep = sep;
	}
	
	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}

	public String getSep() {
		return sep;
	}

	public void setSep(String sep) {
		this.sep = sep;
	}

}
