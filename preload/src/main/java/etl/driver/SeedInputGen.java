package etl.driver;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Monitor the input hdfs directory
 * Generate input file for mapreduce job 
 * Start the mapreduce job
 */
public class SeedInputGen {
	public static final Logger logger = Logger.getLogger(SeedInputGen.class);
	
	private static final String dateFormat = "yyyyMMddHHmmssSSS";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	
	public static final String seedInputFileNameKey="seed.input.filename";
	
	public static void processInput(String hdfsWorkFolder, String rawInputFolder, int inputFilesThreshold, String outputSeedFolder){
		try {
			Date d = new Date();
			FileSystem fs = FileSystem.get(new Configuration());
			String inputFolder = hdfsWorkFolder + "/" + rawInputFolder;
			FileStatus[] files = fs.listStatus(new Path(inputFolder));
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
				String fileName=hdfsWorkFolder + "/" + outputSeedFolder + "/" + "seed" + sdf.format(d);
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
	
	public static void main(String[] args){
		logger.info(Arrays.toString(args));
		String hdfsWorkFolder = args[0];
		String rawInputFolder = args[1];
		int inputFilesThreshold = Integer.parseInt(args[2]);
		String outputSeedFolder = args[3];
		processInput(hdfsWorkFolder, rawInputFolder, inputFilesThreshold, outputSeedFolder);
	}
}
