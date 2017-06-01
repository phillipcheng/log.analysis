package etl.log;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public enum LogMarker {
	AUDIT(MarkerManager.getMarker("AUDIT")),
	ERROR(MarkerManager.getMarker("ERROR"));
	
	public final Marker marker;	
	LogMarker(Marker marker){
		this.marker=marker;
	}
}
