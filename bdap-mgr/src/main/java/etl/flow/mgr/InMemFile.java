package etl.flow.mgr;

public class InMemFile {
	private FileType fileType;
	private String fileName;
	private byte[] content;
	
	public InMemFile(){
	}
	
	public InMemFile(FileType fileType, String fileName, byte[] content){
		this.fileType = fileType;
		this.fileName = fileName;
		this.content = content;
	}
	
	public FileType getFileType() {
		return fileType;
	}
	public String getFileName() {
		return fileName;
	}
	public byte[] getContent() {
		return content;
	}
	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public void setContent(byte[] content) {
		this.content = content;
	}
	
	
}
