package etl.engine;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class ETLLog {
	
	public static final SafeSimpleDateFormat ssdf = new SafeSimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");//2016-03-09T07:45:00
	{
		ssdf.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	private static final int UserCountsNum=4;
	
	private Date start = null;
	private Date end = null;
	private String wfName = "";
	private String wfid = "";
	private String actionName = "";
	private List<String> counts = null;
	
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
		this.wfName = wfName;
	}

	public String getWfid() {
		return wfid;
	}

	public void setWfid(String wfid) {
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

	public String toString(){
		StringBuffer userCounts = new StringBuffer();
		for (int i=0; i<UserCountsNum; i++){
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
		return String.format("%s,%s,%s,%s,%s,%s", ssdf.format(start), ssdf.format(end), wfName, wfid, actionName, userCounts.toString());
	}

}
