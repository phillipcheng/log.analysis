package etl.cmd;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.ParamUtil;
import scala.Tuple2;
import etl.engine.ETLCmd;
import etl.engine.types.ProcessMode;
import etl.util.ConfigKey;


public class HousekeepingCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(HousekeepingCmd.class);
	
	// enum value, will define process type,  hk.type = file  database, it is used to extend later.
	public static final @ConfigKey String cfgkey_hk_type="hk.type";	
	//it is a regular expression, will match with filename, only matched file will be processed
	public static final @ConfigKey String cfgkey_hk_file_include_pattern="hk.file.include.pattern"; 
	//it is a regular expression, will match with filename, matched file will be not processed
	public static final @ConfigKey String cfgkey_hk_file_exclude_pattern="hk.file.exclude.pattern";
	//specified file name date model, will split out datetime by model.  e.g.  yyyyMMdd-HHmmssSSS
	public static final @ConfigKey String cfgkey_hk_filename_model="hk.filename.model";
	//date start index by plus filename, index start from zero
	public static final @ConfigKey(type=Integer.class,defaultValue="-1") String cfgkey_hk_filename_model_index_start="hk.filename.index.start";
	//date start index by reversed filename, index start from zero
	public static final @ConfigKey(type=Integer.class,defaultValue="-1") String cfgkey_hk_filename_model_reversed_index_start="hk.filename.reversed.index.start";
	//will delete by below policy, filename datetime, updatetime     hk.policy=fileName  fileAttr
	public static final @ConfigKey String cfgkey_hk_policy="hk.policy"; 
	//it may be multiple value, split with comma.
	public static final @ConfigKey(type=String[].class) String cfgkey_hk_path="hk.paths";
	//rentention time, unit is minutes, will delete file before the time(include the time), if value is 0, will delete all files.    value must >= 0
	public static final @ConfigKey String cfgkey_hk_rentention_period="hk.rent.period"; 
	// boolean value, if recursive, when select or delete
	public static final @ConfigKey(type=Boolean.class) String cfgkey_hk_recursive="hk.recursive";
	// If you need to delete other files, you need to configure other file's parttern rules. Multiple file rules, separated by commas, for example hk.otherfile.pattern=part-m-(\\d+),_SUCCESS*
	public static final @ConfigKey(type=String[].class) String cfgkey_hk_otherfile_pattern="hk.otherfile.pattern"; 
	
	private String type;
	private String includePattern;
	private String excludePattern;
	private String model;
	private Integer indexStart;
	private Integer reversedIndexStart;
	private String policy;
	private String[] paths;
	private String period;
	private boolean recursive;
	private String[] otherfilePattern;
	
	private final long day=24*60*60*1000;
	private final long hour=60*60*1000;
	private final long minutes=60*1000;

	
	public HousekeepingCmd() {
		super();
	}


	
	public HousekeepingCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs) {
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}

	public HousekeepingCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm) {
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}


	public HousekeepingCmd(String wfName, String wfid, String staticCfg,
			String defaultFs) {
		super(wfName, wfid, staticCfg, defaultFs);
	}
	
	
	@Override
	public void init(String wfName, String wfid, String staticCfg,
			String prefix, String defaultFs, String[] otherArgs, ProcessMode pm) {
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		this.type = super.getCfgString(cfgkey_hk_type, "file");
		this.includePattern = super.getCfgString(cfgkey_hk_file_include_pattern, null);
		this.excludePattern = super.getCfgString(cfgkey_hk_file_exclude_pattern, null);
		this.model = super.getCfgString(cfgkey_hk_filename_model, null);
		this.indexStart = super.getCfgInt(cfgkey_hk_filename_model_index_start, -1);
		this.reversedIndexStart = super.getCfgInt(cfgkey_hk_filename_model_reversed_index_start, -1);
		this.policy = super.getCfgString(cfgkey_hk_policy, "fileAttr");
		this.paths = super.getCfgStringArray(cfgkey_hk_path);
		this.period = super.getCfgString(cfgkey_hk_rentention_period, null);
		this.recursive = super.getCfgBoolean(cfgkey_hk_recursive, false);
		this.otherfilePattern = super.getCfgStringArray(cfgkey_hk_otherfile_pattern);
	}
	
	public List<String> process(String row) throws Exception{
		logger.info(String.format("param: %s", row));
		super.init();
		
		Map<String, String> pm = null;
		try {
			pm = ParamUtil.parseMapParams(row);
		}catch(Exception e){
			logger.error("", e);
		}
		if (pm.containsKey(cfgkey_hk_type)){
			this.type = pm.get(cfgkey_hk_type);
		}
		if (pm.containsKey(cfgkey_hk_file_include_pattern)){
			this.includePattern = pm.get(cfgkey_hk_file_include_pattern);
		}
		if (pm.containsKey(cfgkey_hk_file_exclude_pattern)){
			this.excludePattern = pm.get(cfgkey_hk_file_exclude_pattern);
		}
		if (pm.containsKey(cfgkey_hk_filename_model)){
			this.model = pm.get(cfgkey_hk_filename_model);
		}
		
		if (pm.containsKey(cfgkey_hk_filename_model_index_start)){
			this.indexStart = new Integer(pm.get(cfgkey_hk_filename_model_index_start));
		}
		if (pm.containsKey(cfgkey_hk_filename_model_reversed_index_start)){
			this.reversedIndexStart = new Integer(pm.get(cfgkey_hk_filename_model_reversed_index_start));
		}
		
		if (pm.containsKey(cfgkey_hk_policy)){
			this.policy = pm.get(cfgkey_hk_policy);
		}
		if (pm.containsKey(cfgkey_hk_path)){
			this.paths = new String[]{pm.get(cfgkey_hk_path)};
		}
		if (pm.containsKey(cfgkey_hk_rentention_period)){
			this.period = pm.get(cfgkey_hk_rentention_period);
		}
		if (pm.containsKey(cfgkey_hk_recursive)){
			this.recursive = new Boolean(pm.get(cfgkey_hk_recursive));
		}
		if (pm.containsKey(cfgkey_hk_otherfile_pattern)){
			this.otherfilePattern = new String[]{pm.get(cfgkey_hk_otherfile_pattern)};
		}
		
		List<String> retList = new ArrayList();
		if("file".equals(type)){
			if("fileName".equals(this.policy) && StringUtils.isEmpty(this.model)){
				return null;
			}
			Long currentTime = System.currentTimeMillis();
			Long periodTime = period2Time(this.period);
			for(String path : paths){
				RemoteIterator<LocatedFileStatus> remoteIterator = getFs().listFiles(new Path(path), this.recursive);
				while(remoteIterator.hasNext()){
					LocatedFileStatus fileStatus = remoteIterator.next();
					Path filePath = fileStatus.getPath();
					String fileName = filePath.getName();
					Path parentPath = filePath.getParent();
					boolean isPatternFile = isPatternFile(this.includePattern, this.excludePattern, fileName);
					boolean isOtherFile = isOtherFile(this.otherfilePattern, fileName);
					if(isOtherFile){
						long otherfileTime = fileStatus.getModificationTime();
						if(periodTime >= 0 && (currentTime-otherfileTime) > periodTime){
							getFs().delete(filePath, this.recursive);
							System.out.println(filePath.toString());
							retList.add(filePath.toString());
						}
					}else if(isPatternFile){
						if("fileName".equals(this.policy)){
							if(StringUtils.isNotEmpty(this.model)){
								long fileTime = -1;
								int modelLength = this.model.length();
								if(this.indexStart != -1 && this.indexStart > 0 && (indexStart + modelLength) <= fileName.length()){
									String dateStr = fileName.substring(indexStart, indexStart + modelLength);
									fileTime = string2Time(this.model, dateStr);
								}else if(this.reversedIndexStart != -1 && (fileName.length()-this.reversedIndexStart-modelLength) > 0){
									String dateStr = fileName.substring(fileName.length()-this.reversedIndexStart-modelLength, fileName.length()-this.reversedIndexStart);
									fileTime = string2Time(this.model, dateStr);
								}
								if(fileTime > 0 && (currentTime-fileTime) > periodTime){
									getFs().delete(filePath, this.recursive);
									System.out.println(filePath.toString());
									retList.add(filePath.toString());
								}
							}
							
						}else if("fileAttr".equals(this.policy)){
							long modifyTime = fileStatus.getModificationTime();
							if(periodTime >= 0 && (currentTime-modifyTime) > periodTime){
								getFs().delete(filePath, this.recursive);
								System.out.println(filePath.toString());
								retList.add(filePath.toString());
							}
						}
					}
					//delete parent Path directory
					RemoteIterator<LocatedFileStatus> Iterator = getFs().listFiles(parentPath, this.recursive);
					if(!Iterator.hasNext())
					{
						getFs().delete(parentPath, this.recursive);
						System.out.println(parentPath.toString());
						retList.add(parentPath.toString());
					}

				}
			}
			
			
		}else if("database".equals(type)){
			//in order to extend
		}
		
		return retList;
	}
	
	private boolean patternMatch(String regular, String originStr){
		boolean rs = false;
		try{
//			Pattern pattern = Pattern.compile(regular);
		    Pattern pattern = Pattern.compile(regular, Pattern.CASE_INSENSITIVE);
		    Matcher matcher = pattern.matcher(originStr);
		    rs = matcher.matches();
		}catch(Exception ex){
			logger.error("pattern format is error: " + regular, ex);
		}

		return rs;
	}
	
	private boolean isOtherFile(String[] Regulars, String originStr){
		for (String Regular:Regulars){
			if(StringUtils.isEmpty(Regular)){
				return false;
			}else if(!StringUtils.isEmpty(Regular)){
				if (patternMatch(Regular, originStr))
					{
						return true;
					}
				else 
					continue;
			}
		};
		return false;
	}
	
	private boolean isPatternFile(String includeRegular, String excludeRegular, String originStr){
		if(StringUtils.isEmpty(includeRegular) && StringUtils.isEmpty(excludeRegular)){
			return true;
		}else if(!StringUtils.isEmpty(includeRegular) && StringUtils.isEmpty(excludeRegular)){
			return patternMatch(includeRegular, originStr);
		}else if(StringUtils.isEmpty(includeRegular) && !StringUtils.isEmpty(excludeRegular)){
			return !patternMatch(excludeRegular, originStr);
		}else if(!StringUtils.isEmpty(includeRegular) && !StringUtils.isEmpty(excludeRegular)){
			boolean include = patternMatch(includeRegular, originStr);
			boolean exclude = patternMatch(excludeRegular, originStr);
			if(include && !exclude) {
				return true;
			}else{
				return false;
			}
			
		}
		return false;
	}
	
	private long period2Time(String periodStr){
		//day : d     hour : h    minutes : m
		long periodL = 0;
		if(StringUtils.isEmpty(periodStr)){
			return periodL;
		}
		try{
			periodStr = periodStr.trim();
			if(periodStr.endsWith("d")){
				periodStr = periodStr.replace("d", "");
				periodL = Long.parseLong(periodStr)*this.day;
			}else if(periodStr.endsWith("h")){
				periodStr = periodStr.replace("h", "");
				periodL = Long.parseLong(periodStr)*this.hour;
			}else if(periodStr.endsWith("m")){
				periodStr = periodStr.replace("m", "");
				periodL = Long.parseLong(periodStr)*this.minutes;
			}
			if(periodL < 0){
				periodL = 0;
			}
		}catch(Exception ex){
			periodL = -1;
			logger.error("priod format is error: " + periodStr, ex);
		}
		
		return periodL;
	}
	
	private long string2Time(String formatStr, String dateStr){
		SimpleDateFormat format = new SimpleDateFormat(formatStr);
		long retLong = -1;
		try {
			Date date = format.parse(dateStr);
			retLong = date.getTime();
		} catch (ParseException ex) {
			logger.error("dateStr format is error: " + dateStr, ex);
		}
		return retLong;
	}
	
	@Override
	public boolean hasReduce() {
		return false;
	}


	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName,
			String value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws Exception {
		List<String> fileList = process(value);
		List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
		for (String file:fileList){
			ret.add(new Tuple2<String,String>(file, file));
		}
		return ret;
	}


	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
			//override param
			Map<String, Object> retMap = new HashMap<String, Object>();
			List<String> files = process(row);
			try {
				for (String file: files){
					context.write(new Text(file), null);
				}
			}catch(Exception e){
				logger.error("", e);
			}
			List<String> logInfo = new ArrayList<String>();
			logInfo.add(String.valueOf(files.size()));
			retMap.put(ETLCmd.RESULT_KEY_LOG, logInfo);
			return retMap;
	}


	@Override
	public List<String> sgProcess() throws Exception {
		List<String> ret = process(null);
		int fileNumberDelete=ret.size();
		List<String> logInfo = new ArrayList<String>();
		logInfo.add(fileNumberDelete + "");
		return logInfo;
	}



	public String getType() {
		return type;
	}



	public void setType(String type) {
		this.type = type;
	}



	public String getIncludePattern() {
		return includePattern;
	}



	public void setIncludePattern(String includePattern) {
		this.includePattern = includePattern;
	}



	public String getExcludePattern() {
		return excludePattern;
	}



	public void setExcludePattern(String excludePattern) {
		this.excludePattern = excludePattern;
	}



	public String getModel() {
		return model;
	}



	public void setModel(String model) {
		this.model = model;
	}



	public String getPolicy() {
		return policy;
	}



	public void setPolicy(String policy) {
		this.policy = policy;
	}



	public String[] getPaths() {
		return paths;
	}



	public void setPaths(String[] paths) {
		this.paths = paths;
	}



	public String getPeriod() {
		return period;
	}



	public void setPeriod(String period) {
		this.period = period;
	}



	public boolean isRecursive() {
		return recursive;
	}



	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}



	public Integer getIndexStart() {
		return indexStart;
	}



	public void setIndexStart(Integer indexStart) {
		this.indexStart = indexStart;
	}



	public Integer getReversedIndexStart() {
		return reversedIndexStart;
	}



	public void setReversedIndexStart(Integer reversedIndexStart) {
		this.reversedIndexStart = reversedIndexStart;
	}

}
