package etl.cmd.transform;

public class ColRemover {
	public static final String COMMAND="remove.idx";
	
	private int idx;
	private String rm;
	
	
	public ColRemover(int idx, String rm){
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
