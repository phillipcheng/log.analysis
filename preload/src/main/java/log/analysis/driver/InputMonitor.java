package log.analysis.driver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
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
public class InputMonitor {
	public static final Logger logger = Logger.getLogger(InputMonitor.class);
	
	private static final String dateFormat = "yyyyMMddHHmmssSSS";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
	
	public static void processInput(MRPreloadConfig mrpc){
		try {
			Date d = new Date();
			FileSystem fs = FileSystem.get(new Configuration());
			String inputFolder = mrpc.getHdfsWorkFolder() + "/" + mrpc.getRawInputFolder();
			String processFolder = mrpc.getHdfsWorkFolder() + "/" + mrpc.getProcessingFolder();
			FileStatus[] files = fs.listStatus(new Path(inputFolder));
			if (files.length>=mrpc.getRawInputOutputThreshold()){
				int processFileNumber=mrpc.getRawInputOutputThreshold();
				if (mrpc.getRawInputOutputThreshold()==-1){
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
				String fileName=mrpc.getHdfsWorkFolder() + "/" + mrpc.getRawInputOutputFolder() + "/" + "seed" + sdf.format(d);
				Path fileNamePath = new Path(fileName);
				FSDataOutputStream fin = fs.create(fileNamePath);
				fin.writeBytes(fileContent);
				fin.close();
				
				//start the mr job like a shell program
				String command=mrpc.getHadoopHome() + "\\bin\\hadoop.cmd jar ";
				command += mrpc.getMrprocessJar() + " ";
				command += "log.analysis.driver.MRProcessor" + " ";
				command += fileName;
				CommandLine cmdLine = CommandLine.parse(command);
				DefaultExecutor executor = new DefaultExecutor();
				int exitValue = executor.execute(cmdLine);
				logger.warn(String.format("the exitValue for the mr is:%d", exitValue));
				
				//move the input files to processing files
				for (int i=0; i<inputfiles.size();i++){
					String fn = inputfiles.get(i);
					Path from = new Path(inputFolder + "/" + fn);
					Path to = new Path(processFolder + "/" + fn);
					fs.rename(from, to);
				}
			}
		} catch (IOException e) {
			logger.error("", e);
		}
	}
	
	public static void main(String[] args){
		String mrpreloadProperties = "mrpreload.properties";
		if (args.length>=1){
			mrpreloadProperties=args[0];
		}
		MRPreloadConfig mrpc = new MRPreloadConfig(mrpreloadProperties);
		InputMonitor.processInput(mrpc);
	}
}
