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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import etl.engine.ETLCmd;

/**
 * Monitor the input hdfs directory
 * Generate input file for mapreduce job 
 * Start the mapreduce job
 */
public class GenSeedInputCmd extends ETLCmd{
	
	public static final String cfgkey_workingfolder="working.folder";
	public static final String cfgkey_inputfolder="input.folder";
	public static final String cfgkey_inputfilesthreshold="input.files.threshold";
	public static final String cfgkey_outputseedfolder="output.seed.folder";
	
	private String workingFolder;
	private String inputFolder;
	private int inputFilesThreshold;
	private String outputSeedFolder;
	
	public GenSeedInputCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs) {
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		workingFolder = pc.getString(cfgkey_workingfolder);
		inputFolder = pc.getString(cfgkey_inputfolder);
		inputFilesThreshold = pc.getInt(cfgkey_inputfilesthreshold);
		outputSeedFolder = pc.getString(cfgkey_outputseedfolder);
	}

	public static final Logger logger = Logger.getLogger(GenSeedInputCmd.class);
	
	private static final String dateFormat = "yyyyMMddHHmmssSSS";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	
	public static final String seedInputFileNameKey="seed.input.filename";
	
	@Override
	public void process(String param){
		try {
			Date d = new Date();
			FileSystem fs = FileSystem.get(new Configuration());
			String wiFolder = workingFolder + "/" + inputFolder;
			FileStatus[] files = fs.listStatus(new Path(wiFolder));
			if (files.length>=inputFilesThreshold){
				int processFileNumber=inputFilesThreshold;
				if (inputFilesThreshold==-1){
					processFileNumber = files.length;
				}
				//generate the seed file content
				String fileContent = "";
				List<String> inputfiles = new ArrayList<String>();
				for (int i=0; i<processFileNumber; i++){
					String fn = files[i].getPath().getName();
					fileContent += fn + "\n";
					inputfiles.add(fn);
				}
				//generate the hdfs seed file
				String fileName=workingFolder + "/" + outputSeedFolder + "/" + "seed" + sdf.format(d);
				Path fileNamePath = new Path(fileName);
				FSDataOutputStream fin = fs.create(fileNamePath);
				fin.writeBytes(fileContent);
				fin.close();
				
				//output the seed file as the parameter for further actions in oozie
				File file = new File(System.getProperty("oozie.action.output.properties"));
				Properties props = new Properties();
				props.setProperty(seedInputFileNameKey, fileName);
				OutputStream os = new FileOutputStream(file);
		        props.store(os, "");
		        os.close();
			}
		} catch (IOException e) {
			logger.error("", e);
		}
	}
}
