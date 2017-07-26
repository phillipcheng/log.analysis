package etl.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Locale; 
import java.text.ParseException;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.HdfsUtil;
import bdap.util.PropertiesUtil;
import etl.engine.SafeSimpleDateFormat;

public class GroupFun {
	
	public static final Logger logger = LogManager.getLogger(GroupFun.class);
	
	private static Map<String, SafeSimpleDateFormat> dtMap = new HashMap<String, SafeSimpleDateFormat>();
	
	public static SafeSimpleDateFormat dateSdf = new SafeSimpleDateFormat("yyyy-MM-dd");
	public static SafeSimpleDateFormat hourSdf = new SafeSimpleDateFormat("HH");
	public static ConcurrentHashMap<String, SafeSimpleDateFormat> formatMap = new ConcurrentHashMap<String, SafeSimpleDateFormat>();
	
	///////////////date time related util functions
	public static String hourEpoch(String input){
		Date d = new Date(Long.parseLong(input)*1000);
		return hourSdf.format(d);
	}
	
	public static String dayEpoch(String input){
		Date d = new Date(Long.parseLong(input)*1000);
		return dateSdf.format(d);
	}
	
	public static String dtStandardize(String input, String inputFormat){
		Date d = getStandardizeDt(input, inputFormat);
		if (d!=null){
			return FieldType.sdatetimeFormat.format(d);
		}else{
			return "";
		}
	}
	
	public static Date getStandardizeDt(String input, String inputFormat){
		SafeSimpleDateFormat sdf = null;
		
		if (input != null)
			input = input.trim();
		
		if (input == null || input.length() == 0)
			return null;
		else {
			if (dtMap.containsKey(inputFormat)){
				sdf = dtMap.get(inputFormat);
			}else{
				sdf = new SafeSimpleDateFormat(inputFormat);
				dtMap.put(inputFormat, sdf);
			}
			try {
				Date d = sdf.parse(input);
				return d;
			}catch(Exception e){
				logger.error("", e);
				return null;
			}
		}
	}
	
	public static String convertTimeStampToString(String input, String inputFormat,String timeZone){
		SafeSimpleDateFormat sdf = null;
		if(timeZone == null || "".equals(timeZone)){
			timeZone = "GMT";
		}
		if (input != null)
			input = input.trim();
		if (input == null || input.length() == 0)
			return input;
		else {
			try {
			Date date = new Date(Long.parseLong(input));
			if (dtMap.containsKey(inputFormat)){
				sdf = dtMap.get(inputFormat);
			}else{
				sdf = new SafeSimpleDateFormat(inputFormat);
				dtMap.put(inputFormat, sdf);
			}
			sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
			return sdf.format(date);
			} catch(Exception e){
				logger.error("", e);
				return null;
			}
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
	
	////////////
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
	
	//////////////
	public static String getParentFolderName(String path){
		int slashLastIndex = path.lastIndexOf('/', path.length() - 1);
		if (slashLastIndex>=0){
			String rootToParent = path.substring(0, slashLastIndex);
			return rootToParent.substring(rootToParent.lastIndexOf('/', rootToParent.length() - 1) + 1);
		}else{
			logger.error(String.format("no slash in %s", path));
			return null;
		}
	}
		//////////////
	public static String spiltbysinglespace(String str){
		String newstr = str.replaceAll(" ", ",").trim();
		if(newstr.lastIndexOf(",") == (newstr.length() - 1))
		{
			newstr = newstr.substring(0, newstr.length() - 1);
		}
		return newstr;
	}
		//////////////
	/**
	 * format  e.g. yyyy-MMM-dd hh.mm.ss.S a
	 * @param input
	 * @param inputFormat
	 * @return
	 */
	public static String dtEnglishFormat(String input, String inputFormat){
		SimpleDateFormat format = new SimpleDateFormat(inputFormat,  Locale.ENGLISH);
		String returnStr = "";
		try {
			Date d = format.parse(input);
			if (d!=null){
				returnStr = FieldType.sdatetimeFormat.format(d);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return returnStr;
	}
	///////////////
	public static String subnumber(String hexStr, int beginByteIndex, int endByteIndex, String defaultValue) {
		byte[] byteArray=hexStringToByteArray(hexStr);
		if(byteArray==null){
			return defaultValue;
		}
		
		int beginIndex=0;
		int endIndex=0;
		
		if(beginByteIndex>(byteArray.length-1)){
			return defaultValue;
		}
		beginIndex=byteArray.length-1-endByteIndex;
		if(beginIndex<0) beginIndex=0;
		endIndex=byteArray.length-1-beginByteIndex;
		
		
		long value=0;
		for(int i=beginIndex;i<=endIndex;i++){
			value=(value << 8) | byteArray[i];
		}

		return String.valueOf(value);
	}
	
	public static byte[] hexStringToByteArray(String hexStr) {
		if(hexStr==null) return null;
		hexStr=hexStr.toUpperCase();
		if(hexStr.startsWith("0X")) hexStr=hexStr.substring(2);
		if(hexStr.isEmpty()) return null;
		if(hexStr.length()%2 ==1) hexStr="0"+hexStr;
		return DatatypeConverter.parseHexBinary(hexStr);
	}
	
	//////////////////
	public static String[] getValues(String defaultFs, String dfsFile){
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", defaultFs);
			FileSystem fs = FileSystem.get(conf);
			List<String> strs = HdfsUtil.stringsFromDfsFile(fs, dfsFile);
			if (strs.size()==0){
				return null;
			}else{
				return strs.get(0).split(",");
			}
		}catch(Exception e){
			logger.error(String.format("error getValues from %s", dfsFile), e);
			return null;
		}
	}
	
	public static String[] splitTimeRange(String startTimeStr,String endTimeStr,String inputTimeFormat, String inputTimezone, String outputTimeFormat, long splitSize){
		try{
			SimpleDateFormat sdf=new SimpleDateFormat(inputTimeFormat);
			sdf.setTimeZone(TimeZone.getTimeZone(inputTimezone));
			SimpleDateFormat sdfOut=new SimpleDateFormat(outputTimeFormat);
			sdfOut.setTimeZone(TimeZone.getTimeZone("UTC"));
			
			Date startTime=sdf.parse(startTimeStr);
			Date endTime=sdf.parse(endTimeStr);
			
			long rangeStart= (startTime.getTime()/splitSize)*splitSize;
			
			StringBuilder sb=new StringBuilder();
			List<String> splitedDateList=new ArrayList<String>();
			long splitRangeStart=rangeStart;
			while(splitRangeStart<=endTime.getTime()){
				sb.append(sdfOut.format(new Date(splitRangeStart))).append(",").append(sdfOut.format(new Date(splitRangeStart+splitSize-1)));
				splitedDateList.add(sb.toString());
				sb.setLength(0);
				splitRangeStart=splitRangeStart+splitSize;
			}
			
			return splitedDateList.toArray(new String[0]);
		}catch(Exception e){
			logger.error("Cannot split the time range with startTime:{}, endTime:{}, inputTimeFormat:{}, inputTimeZone:{}, outputTimeFormat:{}, splitSize:{}",new Object[]{startTimeStr,endTimeStr,inputTimeFormat, inputTimezone, outputTimeFormat, splitSize});
			logger.error("With Exception:",e);
			return null;
		}
		
	}
	
	public static String getSubString(String str,String formatStr){
		if(str == null){
			return null;
		}
		Pattern pattern = Pattern.compile(formatStr);
        Matcher  m = pattern.matcher(str);
        if(m.matches()){
            return m.group(1);
        }
        return null;
	}
	
	public static String dateSubtract(String startDate ,String endDate){
		try {
			if(startDate == null || endDate == null){
				return null;
			}
			Date dateEnd = FieldType.sdatetimeFormat.parse(endDate);
			Date dateBegin = FieldType.sdatetimeFormat.parse(startDate);
			if(dateEnd == null || dateBegin == null){
				return null;
			}
			return String.valueOf((dateEnd.getTime() - dateBegin.getTime())/1000);
		} catch (Exception e) {
			logger.error("", e.getMessage());
		}
		return null;
		
	}
}
