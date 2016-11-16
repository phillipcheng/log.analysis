package bdap.tools.pushagent;

public class DirConfig {
	private String id;
	private String directory;
	private Element[] elements;
	private String category;
	private String timeZone;
	private String cronExpr;
	private boolean recursive; /* Recursive in sub directories */
	private String filenameFilterExpr;
	private int filesPerBatch; /* Number of files to transfer per batch process, 0 means disable check */
	private String processRecordFile;
	private String destServer;
	private int destServerPort;
	private String destServerUser;
	private String destServerPass;
	private String destServerDirRule;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDirectory() {
		return directory;
	}
	public void setDirectory(String directory) {
		this.directory = directory;
	}
	public Element[] getElements() {
		return elements;
	}
	public void setElements(Element[] elements) {
		this.elements = elements;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getTimeZone() {
		return timeZone;
	}
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}
	public String getCronExpr() {
		return cronExpr;
	}
	public void setCronExpr(String cronExpr) {
		this.cronExpr = cronExpr;
	}
	public String getDestServer() {
		return destServer;
	}
	public void setDestServer(String destServer) {
		this.destServer = destServer;
	}
	public int getDestServerPort() {
		return destServerPort;
	}
	public void setDestServerPort(int destServerPort) {
		this.destServerPort = destServerPort;
	}
	public String getDestServerUser() {
		return destServerUser;
	}
	public void setDestServerUser(String destServerUser) {
		this.destServerUser = destServerUser;
	}
	public String getDestServerPass() {
		return destServerPass;
	}
	public void setDestServerPass(String destServerPass) {
		this.destServerPass = destServerPass;
	}
	public String getDestServerDirRule() {
		return destServerDirRule;
	}
	public void setDestServerDirRule(String destServerDirRule) {
		this.destServerDirRule = destServerDirRule;
	}
	public String getFilenameFilterExpr() {
		return filenameFilterExpr;
	}
	public void setFilenameFilterExpr(String filenameFilterExpr) {
		this.filenameFilterExpr = filenameFilterExpr;
	}
	public boolean isRecursive() {
		return recursive;
	}
	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}
	public int getFilesPerBatch() {
		return filesPerBatch;
	}
	public void setFilesPerBatch(int filesPerBatch) {
		this.filesPerBatch = filesPerBatch;
	}
	public String getProcessRecordFile() {
		return processRecordFile;
	}
	public void setProcessRecordFile(String processRecordFile) {
		this.processRecordFile = processRecordFile;
	}
}
