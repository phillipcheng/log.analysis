package etl.flow.mgr;

import java.util.Arrays;
import java.util.Date;

public class NodeInfo {
	private String nodeId;
	private String nodeName;
	private String type;
	private String status;
	private String[] outputFilePaths;
	private Date startTime;
	private Date endTime;
	
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public Date getEndTime() {
		return endTime;
	}
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	public String[] getOutputFilePaths() {
		return outputFilePaths;
	}
	public void setOutputFilePaths(String[] outputFilePaths) {
		this.outputFilePaths = outputFilePaths;
	}

	public String toString() {
		return "NodeInfo [nodeId=" + nodeId + ", nodeName=" + nodeName + ", type=" + type + ", status=" + status
				+ ", outputFilePaths=" + Arrays.toString(outputFilePaths) + ", startTime=" + startTime + ", endTime="
				+ endTime + "]";
	}
}
