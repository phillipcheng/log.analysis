package etl.flow;

import java.time.format.DateTimeFormatter;

public class CoordConf {
	
	public static String sdformat="yyyy-MM-dd'T'hh:mm:ss";
	public static DateTimeFormatter df = DateTimeFormatter.ofPattern(sdformat);
	
	private String startTime;
	private String endTime;
	private int duration; //unit minute
	private String coordPath;
	private String timeZone;
	
	public CoordConf(String startTime, String endTime, int duration){
		this.startTime= startTime;
		this.endTime = endTime;
		this.duration = duration;
	}
	
	public String getStartTime() {
		return startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public int getDuration() {
		return duration;
	}
	public String getTimeZone() {
		return timeZone;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}
	public String getCoordPath() {
		return coordPath;
	}
	public void setCoordPath(String coordPath) {
		this.coordPath = coordPath;
	}
}

