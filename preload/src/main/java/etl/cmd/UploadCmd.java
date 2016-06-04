package etl.cmd;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;

public class UploadCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(UploadCmd.class);
	
	public static final String cfgkey_from_dir = "from.dir";
	public static final String cfgkey_file_filter = "file.filter";
	public static final String cfgkey_xml_folder="xml-folder";
	
	private String xmlFolder;
	private String fromDir;
	private String fileExp;
	
	public UploadCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.xmlFolder = pc.getString(cfgkey_xml_folder);
		this.fromDir = pc.getString(cfgkey_from_dir);
		this.fileExp = pc.getString(cfgkey_file_filter);
	}
	
	@Override
	public void process(String param) {
		try {
			File dir = new File(fromDir);
			FileFilter fileFilter = new WildcardFileFilter(fileExp);
			File[] files = dir.listFiles(fileFilter);
			for (int i = 0; i < files.length; i++) {
			   File f = files[i];
			   fs.copyFromLocalFile(new Path(fromDir+f.getName()), new Path(xmlFolder+f.getName()));
			   logger.info(String.format("copyed %s to %s", fromDir+f.getName(), xmlFolder+f.getName()));
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
}