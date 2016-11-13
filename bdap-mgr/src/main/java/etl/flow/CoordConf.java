package etl.flow;

import java.time.format.DateTimeFormatter;

public class CoordConf {
	
	public static String sdformat="yyyy-MM-dd'T'hh:mm'Z'";
	public static DateTimeFormatter df = DateTimeFormatter.ofPattern(sdformat);
	
	private String startTime;
	private String endTime;
	private String duration; //unit minute
	private String coordPath;
	private String timeZone;
	
	public CoordConf(String startTime, String endTime, String duration){
		this.startTime= startTime;
		this.endTime = endTime;
		this.setDuration(duration);
	}
	
	public String getStartTime() {
		return startTime;
	}
	public String getEndTime() {
		return endTime;
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
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}
	public String getCoordPath() {
		return coordPath;
	}
	public void setCoordPath(String coordPath) {
		this.coordPath = coordPath;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}
}

