package etl.util;

import java.io.File;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.PropertiesConfiguration;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import etl.engine.SafeSimpleDateFormat;

public class GroupFun {
	
	public static final Logger logger = LogManager.getLogger(GroupFun.class);
	
	private static Map<String, SafeSimpleDateFormat> dtMap = new HashMap<String, SafeSimpleDateFormat>();
	
	public static SafeSimpleDateFormat dateSdf = new SafeSimpleDateFormat("yyyy-MM-dd");
	public static SafeSimpleDateFormat hourSdf = new SafeSimpleDateFormat("HH");
	public static ConcurrentHashMap<String, SafeSimpleDateFormat> formatMap = new ConcurrentHashMap<String, SafeSimpleDateFormat>();
	
	public static String hourEpoch(String input){
		Date d = new Date(Long.parseLong(input)*1000);
		return hourSdf.format(d);
	}
	
	public static String dayEpoch(String input){
		Date d = new Date(Long.parseLong(input)*1000);
		return dateSdf.format(d);
	}
	
	public static String dtStandardize(String input, String inputFormat){
		SafeSimpleDateFormat sdf = null;
		if (dtMap.containsKey(inputFormat)){
			sdf = dtMap.get(inputFormat);
		}else{
			sdf = new SafeSimpleDateFormat(inputFormat);
			dtMap.put(inputFormat, sdf);
		}
		try {
			Date d = sdf.parse(input);
			return FieldType.sdatetimeFormat.format(d);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
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
	
	public static String getDateTime(String format){
		if (!formatMap.containsKey(format)){
			formatMap.put(format, new SafeSimpleDateFormat(format));
		}
		SafeSimpleDateFormat ssdt = formatMap.get(format);
		return ssdt.format(new Date());
	}
	
	//change the date part of the epoch (in seconds) to the current date
	public static String changeDateToCurrent(String epochInSec){
		Calendar inputCal = Calendar.getInstance();
		inputCal.setTimeInMillis(Long.parseLong(epochInSec)*1000);
		Calendar currentCal = Calendar.getInstance();
		inputCal.set(Calendar.YEAR, currentCal.get(Calendar.YEAR));
		inputCal.set(Calendar.MONTH, currentCal.get(Calendar.MONTH));
		inputCal.set(Calendar.DATE, currentCal.get(Calendar.DATE));
		long outputSec = inputCal.getTimeInMillis()/1000;
		return String.valueOf(outputSec);
	}
	
	public static Map<String, String> getMap(String mappingFile, String keyKey, String valueKey){
		logger.info(String.format("mapping file:%s", mappingFile));
		PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(mappingFile);
		String[] keys = pc.getStringArray(keyKey);
		String[] values = pc.getStringArray(valueKey);
		logger.info(String.format("keys:%s", Arrays.asList(keys)));
		logger.info(String.format("values:%s", Arrays.asList(values)));
		Map<String, String> mapping = new HashMap<String, String>();
		for (int i=0; i<keys.length; i++){
			mapping.put(keys[i], values[i]);
		}
		return mapping;
	}
	
	public static String getParentFolderName(String path){
	    String rootToParent = path.substring(0, path.lastIndexOf('/', path.length() - 1));
	    return rootToParent.substring(rootToParent.lastIndexOf('/', rootToParent.length() - 1) + 1);
	}
}
