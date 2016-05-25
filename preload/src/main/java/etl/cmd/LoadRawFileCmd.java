package etl.cmd;

import java.io.File;
import java.io.FileFilter;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.engine.ETLCmd;
import etl.util.Util;

public class LoadRawFileCmd implements ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);
	private String wfid;
	private FileSystem fs;
	private PropertiesConfiguration pc;
	private String xmlFolder;
	
	public LoadRawFileCmd(String defaultFs, String wfid, String staticCfg, String dynCfg){
		this.wfid = wfid;
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", defaultFs);
			this.fs = FileSystem.get(conf);
		}catch(Exception e){
			logger.error("", e);
		}
		this.pc = Util.getPropertiesConfigFromDfs(fs, staticCfg);
		this.xmlFolder = pc.getString(DynSchemaCmd.cfgkey_xml_folder);
	}
	
	@Override
	public void process() {
		try {
			String strDir = "C:\\mydoc\\myprojects\\log.analysis\\mtccore\\working\\xmldata\\";
			String fileExp = "*-NK-*";
			File dir = new File(strDir);
			FileFilter fileFilter = new WildcardFileFilter(fileExp);
			File[] files = dir.listFiles(fileFilter);
			for (int i = 0; i < files.length; i++) {
			   File f = files[i];
			   fs.copyFromLocalFile(new Path(strDir+f.getName()), new Path(xmlFolder+f.getName()));
			}
		}catch(Exception e){
			logger.error("", e);
		}
	}
}