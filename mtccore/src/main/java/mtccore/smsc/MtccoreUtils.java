package mtccore.smsc;

import java.util.List;

import org.apache.log4j.Logger;

import etl.engine.LogicSchema;
import etl.util.DBUtil;
import etl.util.KeyValueMapping;

public class MtccoreUtils {
	public static final Logger logger = Logger.getLogger(MtccoreUtils.class);
	private static KeyValueMapping kvm= new KeyValueMapping("smsc_file_table_mapping.properties", "table.names", "filebase.names");
	
	public static String getCsvFile(String tableName, String wfid){
		String fileNameBase = kvm.getValue(tableName);
		logger.info(String.format("tableName:%s, wfid:%s, fileNameBase:%s", tableName, wfid, fileNameBase));
		String csvFile = String.format("/mtccore/smscraw/csv/%s/%s.*", wfid, fileNameBase);
		return csvFile;
	}
	
	public static String getCopySql(LogicSchema ls, String tableName, String csvFileName, 
			String dbPrefix, String username, String rootWebHdfs, String dbType){
		logger.info(String.format("csvFileName:%s", csvFileName));
		List<String> attrs = ls.getAttrNames(tableName);
		if (tableName.endsWith("_merge")){
			return DBUtil.genCopyHdfsSql(null, attrs, tableName, dbPrefix, rootWebHdfs, csvFileName, username, dbType);
		}else{
			attrs.remove("SchedTime");
			attrs.remove("ActualTime");
			String prefix = "epochT1 FILLER VARCHAR(15),epochT2 FILLER VARCHAR(15),SchedTime AS TO_TIMESTAMP(epochT1),ActualTime AS TO_TIMESTAMP(epochT2),";
			return DBUtil.genCopyHdfsSql(prefix, attrs, tableName, dbPrefix, rootWebHdfs, csvFileName, username, dbType);
		}
	}
}
