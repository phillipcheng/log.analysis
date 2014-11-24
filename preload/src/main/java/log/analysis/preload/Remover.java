package log.analysis.preload;

public class Remover {
	private int idx;
	private String rm;
	
	public Remover(int idx, String rm){
		this.idx = idx;
		this.setRm(rm);
	}
	
	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}

	public String getRm() {
		return rm;
	}

	public void setRm(String rm) {
		this.rm = rm;
	}


}
