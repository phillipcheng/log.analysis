package etl.log;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import etl.cmd.SendLogCmd;
import etl.engine.ETLCmd;
import etl.engine.SafeSimpleDateFormat;

public class ETLLog implements Serializable{
	private static final long serialVersionUID = 1L;

	public static final SafeSimpleDateFormat ssdf = new SafeSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");//2016-03-09T07:45:00
	{
		ssdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	private static final int UserCountsNum=4;
	
	private LogType type = null;
	private Date start = null;
	private Date end = null;
	private String wfName = "";
	private String wfid = "";
	private String actionName = "";
	private List<String> counts = null;
	private String exception;
	
	public ETLLog(LogType type){
		this.type = type;
	}
	
	public ETLLog(String wfName, String wfId, String cmdClass, String exception, Throwable e){
		this.wfName = wfName;
		this.wfid = wfId;
		this.actionName = cmdClass;
		//
		this.type = LogType.etlexception;
		this.start = new Date();
		this.end =start;
		if (exception==null){
			this.exception = ExceptionUtils.getStackTrace(e);
		}else if (e==null){
			this.exception = exception;
		}else{
			this.exception = String.format("msg:%s, trace:%s", exception, ExceptionUtils.getStackTrace(e));
		}
	}
	
	public ETLLog(ETLCmd cmd, String exception, Throwable e){
		this.wfName = cmd.getWfName();
		this.wfid = cmd.getWfid();
		this.actionName = cmd.getClass().getName();
		//
		this.type = LogType.etlexception;
		this.start = new Date();
		this.end =start;
		if (exception==null){
			this.exception = ExceptionUtils.getStackTrace(e);
		}else if (e==null){
			this.exception = exception;
		}else{
			this.exception = String.format("msg:%s, trace:%s", exception, ExceptionUtils.getStackTrace(e));
		}
	}
	
	public ETLLog(String exception){
		//
		this.type = LogType.etlexception;
		this.start = new Date();
		this.end =start;
		this.exception = exception;
	}
	
	public Date getStart() {
		return start;
	}

	public void setStart(Date start) {
		this.start = start;
	}

	public Date getEnd() {
		return end;
	}

	public void setEnd(Date end) {
		this.end = end;
	}

	public String getWfName() {
		return wfName;
	}

	public void setWfName(String wfName) {
		if (wfName!=null)
			this.wfName = wfName;
	}

	public String getWfid() {
		return wfid;
	}

	public void setWfid(String wfid) {
		if (wfid!=null)
			this.wfid = wfid;
	}

	public String getActionName() {
		return actionName;
	}

	public void setActionName(String actionName) {
		this.actionName = actionName;
	}

	public List<String> getCounts() {
		return counts;
	}

	public void setCounts(List<String> counts) {
		this.counts = counts;
	}
	
	public LogType getType() {
		return type;
	}

	public void setType(LogType type) {
		this.type = type;
	}
	
	public String getException() {
		return exception;
	}

	public void setException(String exception) {
		this.exception = exception;
	}

	public String getCsv(List<String> counts, int maxCount){
		StringBuffer userCounts = new StringBuffer();
		for (int i=0; i<maxCount; i++){
			if (counts!=null && i<counts.size()){
				if (i==0){
					userCounts.append(counts.get(i));
				}else{
					userCounts.append(",").append(counts.get(i));
				}	
			}else{
				if (i==0){
					userCounts.append("");
				}else{
					userCounts.append(",");
				}
			}
		}
		return userCounts.toString();
	}
	
	public String toString(){
		if (LogType.etlexception==type){
			return String.format("%s,%s,%s,%s,%s", ssdf.format(start), wfName, wfid, actionName, exception);
		}else{
			String userCounts;
			if (actionName.contains(SendLogCmd.class.getName())){
				userCounts = getCsv(counts, UserCountsNum+1); //for SendLogCmd, the actionName will be passed in via counts
				return String.format("%s,%s,%s,%s,%s", ssdf.format(start), ssdf.format(end), wfName, wfid, userCounts.toString());
			}else{
				userCounts = getCsv(counts, UserCountsNum);
				return String.format("%s,%s,%s,%s,%s,%s", ssdf.format(start), ssdf.format(end), wfName, wfid, actionName, userCounts.toString());
			}
		}
	}
}
