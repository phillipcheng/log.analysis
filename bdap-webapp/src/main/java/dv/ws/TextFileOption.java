package dv.ws;

public class TextFileOption {
	private String filePath;
	private long startLine;
	private long endLine;
	public long getStartLine() {
		return startLine;
	}
	public void setStartLine(long startLine) {
		this.startLine = startLine;
	}
	public long getEndLine() {
		return endLine;
	}
	public void setEndLine(long endLine) {
		this.endLine = endLine;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
}
