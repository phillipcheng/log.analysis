package etl.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HdfsUtil {
	
	public static final Logger logger = LogManager.getLogger(HdfsUtil.class);

	public static FileSystem getHadoopFs(String defaultFs){
		String fs_key = "fs.defaultFS";
		Configuration conf = new Configuration();
		if (defaultFs!=null){
			conf.set(fs_key, defaultFs);
		}
		logger.info(String.format("%s is %s", fs_key, conf.get(fs_key)));
		try {
			return FileSystem.get(conf);
		} catch (IOException e) {
			logger.error("", e);
			return null;
		}
	}
	
	
	
	public static int writeDfsFile(FileSystem fs, String fileName, Iterable<String> contents){
		BufferedWriter osw = null;
		try {
			osw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(fileName))));
			int i=0;
			for (String line:contents){
				i++;
				osw.write(line);
				osw.write("\n");
			}
			logger.info(String.format("write %d lines to file:%s", i, fileName));
			return i;
		}catch(Exception e){
			logger.error("",e);
		}finally{
			if (osw!=null){
				try {
					osw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
		return 0;
	}
	
	public static List<String> stringsFromDfsFile(FileSystem fs, String file){
		BufferedReader in = null;
		List<String> sl = new ArrayList<String>();
		try {
			in = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
			String s =null;
			while ((s=in.readLine())!=null){
				sl.add(s);
			}
			return sl;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}finally{
			if (in!=null){
				try{
					in.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	/*
	 * if fileName exists, append the contents to org content into fileName+fileNameAppend return true
	   if not exists, create file put content, return false
	 */
	public static void appendDfsFile(FileSystem fs, String fileName, List<String> contents){
		BufferedWriter osw = null;
		try {
			List<String> allContents = new ArrayList<String>();
			if (fs.exists(new Path(fileName))){
				List<String> orgContents = stringsFromDfsFile(fs, fileName);
				allContents.addAll(orgContents);
			}
			allContents.addAll(contents);
			writeDfsFile(fs, fileName, allContents);
		}catch(Exception e){
			logger.error("",e);
		}finally{
			if (osw!=null){
				try {
					osw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static List<String> listDfsFile(FileSystem fs, String folder){
		List<String> files = new ArrayList<String>();
		try {
			FileStatus[] fslist = fs.listStatus(new Path(folder));
			for (FileStatus f:fslist){
				files.add(f.getPath().getName());
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return files;
	}
	
	//return all the content of the files under the foler
	public static List<String> stringsFromDfsFolder(FileSystem fs, String folder){
		List<String> output = new ArrayList<String>();
		try {
			FileStatus[] fsts = fs.listStatus(new Path(folder));
			if (fsts!=null){
				for (FileStatus fst:fsts){
					if (fst.isFile()){
						BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(fst.getPath())));
						String line = null;
						while ((line=in.readLine())!=null){
							output.add(line);
						}
						in.close();
					}else{
						output.addAll(stringsFromDfsFolder(fs, fst.getPath().toString()));
					}
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return output;
	}
}
