package etl.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtil {
	
	public static String getWeekOfDayForYesterday(){
		DateFormat dateFormat = new SimpleDateFormat("EEEE");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return (dateFormat.format(cal.getTime())).toLowerCase();
	}
	
	public static String getMonday(){
		return "monday";
	}

}
