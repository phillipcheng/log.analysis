package etl.cmd;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;

/**
 * Monitor the input hdfs directory
 * Generate input file for mapreduce job 
 * Start the mapreduce job
 */
public class GenSeedInputCmd extends ETLCmd{
	public static final String cfgkey_inputfolder="input.folder";
	public static final String cfgkey_outputseedfolder="output.seed.folder";
	public static final String cfgkey_usewfid="use.wfid";
	
	private String inputFolder;
	private boolean useWfid = false;//for inputFolder
	private String outputSeedFolder;
	
	public GenSeedInputCmd(String wfid, String staticCfg, String dynCfg, String defaultFs, String[] otherArgs){
		super(wfid, staticCfg, dynCfg, defaultFs, otherArgs);
		inputFolder = pc.getString(cfgkey_inputfolder);
		useWfid = pc.getBoolean(cfgkey_usewfid, false);
		outputSeedFolder = pc.getString(cfgkey_outputseedfolder);
	}

	public static final Logger logger = Logger.getLogger(GenSeedInputCmd.class);
	
	private static final String dateFormat = "yyyyMMddHHmmssSSS";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	
	public static final String seedInputFileNameKey="seed.input.filename";
	
	@Override
	public List<String> sgProcess() {
		List<String> logInfo = new ArrayList<String>();
		try {
			Date d = new Date();
			String wiFolder = inputFolder;
			if (useWfid){
				wiFolder = wiFolder + wfid;
			}
			FileStatus[] files = fs.listStatus(new Path(wiFolder));
			int processFileNumber=files.length;
			//generate the seed file content
			String fileContent = "";
			List<String> inputfiles = new ArrayList<String>();
			for (int i=0; i<processFileNumber; i++){
				String fn = files[i].getPath().getName();
				fileContent += fn + "\n";
				inputfiles.add(fn);
			}
			//generate the hdfs seed file
			String fileName= outputSeedFolder + "/" + "seed" + sdf.format(d);
			Path fileNamePath = new Path(fileName);
			FSDataOutputStream fin = fs.create(fileNamePath);
			fin.writeBytes(fileContent);
			fin.close();
			
			//output the seed file as the parameter for further actions in oozie
			String oozieOutputProperties = System.getProperty("oozie.action.output.properties");
			if (oozieOutputProperties!=null){
				File file = new File(oozieOutputProperties);
				Properties props = new Properties();
				props.setProperty(seedInputFileNameKey, fileName);
				OutputStream os = new FileOutputStream(file);
		        props.store(os, "");
		        os.close();
			}
			logInfo.add(inputfiles.size()+"");
			
		} catch (IOException e) {
			logger.error("", e);
		}
		return logInfo;
	}
}
