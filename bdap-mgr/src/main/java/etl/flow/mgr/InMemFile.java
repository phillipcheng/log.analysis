package etl.flow.mgr;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonInclude;

import bdap.util.FileType;

public class InMemFile {
	private FileType fileType;
	private String fileName;
	private byte[] content;
	private String textContent;
	private boolean partial;
	
	public InMemFile(){
	}
	
	public InMemFile(FileType fileType, String fileName, byte[] content){
		this.fileType = fileType;
		this.fileName = fileName;
		this.content = content;
	}
	
	public InMemFile(FileType fileType, String fileName, byte[] content, int contentSize){
		this.fileType = fileType;
		this.fileName = fileName;
		this.content = Arrays.copyOf(content, contentSize);
	}
	
	public InMemFile(FileType fileType, String fileName, String textContent) {
		this.fileType = fileType;
		this.fileName = fileName;
		this.textContent = textContent;
	}

	public InMemFile(FileType fileType, String fileName, String textContent, boolean partial) {
		this.fileType = fileType;
		this.fileName = fileName;
		this.textContent = textContent;
		this.partial = partial;
	}

	public String toString(){
		return String.format("type:%s,name:%s", fileType, fileName);
	}
	
	public FileType getFileType() {
		return fileType;
	}
	
	public String getFileName() {
		return fileName;
	}
	
	@JsonInclude(JsonInclude.Include.NON_NULL) 
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
	
	@JsonInclude(JsonInclude.Include.NON_NULL) 
	public String getTextContent() {
		return textContent;
	}
	
	public void setTextContent(String textContent) {
		this.textContent = textContent;
	}

	public boolean isPartial() {
		return partial;
	}

	public void setPartial(boolean partial) {
		this.partial = partial;
	}
}
