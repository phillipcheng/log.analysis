package etl.cmd.transform;

import java.util.regex.Pattern;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class KcvToCsvConf {
	public static final Logger logger = Logger.getLogger(KcvToCsvConf.class);
	
	//record format overall specification
	public static final String RECORD_START="record.start";
		public static final String RECORD_SINGLELINE="^"; //single line

	public static final String RECORD_KCV_VK_REGEXP="record.vkexp";
		
	public static final String RECORD_FIELDNUM="record.fieldnum";
		public static final int RECORD_FIELDNUM_DEFAULT=-1; //extract all fields recognized

	//record format definition
	private String recordStart = RECORD_SINGLELINE;
	private Pattern recordStartPattern = null;
	private Pattern recordVKExp = null;
	private int recordFieldNum = RECORD_FIELDNUM_DEFAULT;
	
	public KcvToCsvConf(PropertiesConfiguration pc){
		//record overall configuration
		String strVal = pc.getString(KcvToCsvConf.RECORD_START);
		if (strVal!=null){
			this.recordStart = strVal;
			this.setRecordStartPattern(Pattern.compile(strVal));
		}

		strVal = pc.getString(KcvToCsvConf.RECORD_KCV_VK_REGEXP);
		if (strVal!=null){
			this.recordVKExp = Pattern.compile(strVal);
		}
		
		strVal = pc.getString(KcvToCsvConf.RECORD_FIELDNUM);
		if (strVal!=null){
			this.recordFieldNum = Integer.parseInt(strVal);
		}
	}

	public String getRecordStart() {
		return recordStart;
	}

	public void setRecordStart(String recordStart) {
		this.recordStart = recordStart;
	}

	public int getRecordFieldNum() {
		return recordFieldNum;
	}

	public void setRecordFieldNum(int recordFieldNum) {
		this.recordFieldNum = recordFieldNum;
	}

	public Pattern getRecordStartPattern() {
		return recordStartPattern;
	}

	public void setRecordStartPattern(Pattern recordStartPattern) {
		this.recordStartPattern = recordStartPattern;
	}

	public Pattern getRecordVKExp() {
		return recordVKExp;
	}

	public void setRecordVKExp(Pattern recordVKExp) {
		this.recordVKExp = recordVKExp;
	}
}
