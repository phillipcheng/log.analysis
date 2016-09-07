package etl.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import etl.engine.SafeSimpleDateFormat;

public class GroupFun {
	
	public static final Logger logger = Logger.getLogger(GroupFun.class);
	
	private static Map<String, SafeSimpleDateFormat> dtMap = new HashMap<String, SafeSimpleDateFormat>();
	
	public static SafeSimpleDateFormat dateSdf = new SafeSimpleDateFormat("yyyy-MM-dd");
	public static SafeSimpleDateFormat hourSdf = new SafeSimpleDateFormat("HH");
	
	public static String hourEpoch(String input){
		Date d = new Date(Long.parseLong(input)*1000);
		return hourSdf.format(d);
	}
	
	public static String dayEpoch(String input){
		Date d = new Date(Long.parseLong(input)*1000);
		return dateSdf.format(d);
	}
	
	public static String hour(String input, String dtFormat){
		SafeSimpleDateFormat sdf = null;
		if (dtMap.containsKey(dtFormat)){
			sdf = dtMap.get(dtFormat);
		}else{
			sdf = new SafeSimpleDateFormat(dtFormat);
			dtMap.put(dtFormat, sdf);
		}
		try {
			Date d = sdf.parse(input);
			return hourSdf.format(d);
		}catch(Exception e){
			logger.error("", e);
		}
		return "error";
	}
	
	public static String day(String input, String dtFormat){
		SafeSimpleDateFormat sdf = null;
		if (dtMap.containsKey(dtFormat)){
			sdf = dtMap.get(dtFormat);
		}else{
			sdf = new SafeSimpleDateFormat(dtFormat);
			dtMap.put(dtFormat, sdf);
		}
		try {
			Date d = sdf.parse(input);
			return dateSdf.format(d);
		}catch(Exception e){
			logger.error("", e);
		}
		return "error";
	}
	
	public static String getDateTime(){
		Date d = new Date();
		return FieldType.sdatetimeFormat.format(d);
	}
}
