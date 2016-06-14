package etl.cmd;

import java.util.List;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.engine.ETLCmd;

public class BackupCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);
	
	private String xmlFolder;
	private String dataHistoryFolder;
	
	public BackupCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.xmlFolder = pc.getString(DynSchemaCmd.cfgkey_xml_folder);
		this.dataHistoryFolder = pc.getString(DynSchemaCmd.cfgkey_data_history_folder);
	}
	
	@Override
	public List<String> process(String param) {
		List<String> xmlFiles = dynCfgMap.get(DynSchemaCmd.dynCfg_Key_XML_FILES);
		try {
			for (String xmlFile: xmlFiles){
				FileUtil.copy(fs, new Path(xmlFolder+xmlFile), fs, new Path(dataHistoryFolder+xmlFile), true, this.getHadoopConf());
				logger.info(String.format("copy and remove %s to %s", xmlFolder+xmlFile, dataHistoryFolder+xmlFile));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return null;
	}
}
