package bdap.util;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util {
	public static final Logger logger = LogManager.getLogger(Util.class);
	
	public static String getCsv(List<String> csv, boolean newline){
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<csv.size(); i++){
			String v = (String) csv.get(i);
			if (v!=null){
				sb.append(v);
			}
			if (i<csv.size()-1){
				sb.append(",");
			}
		}
		if (newline){
			sb.append("\n");
		}
		return sb.toString();
	}
	
	public static void writeFile(String fileName, String contents){
		writeFile(fileName, Arrays.asList(new String[]{contents}));
	}
	
	public static void writeFile(String fileName, List<String> contents){
		BufferedWriter osw = null;
		try {
			osw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
			for (String line:contents){
				osw.write(line);
				osw.write("\n");
			}
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
	
	//zip
	public static void zipFiles(ZipOutputStream zos, String inputFolder, List<String> inputFileNames) throws IOException {
		byte[] buffer = new byte[1024];
		for(String file : inputFileNames){
    		ZipEntry ze= new ZipEntry(file);
        	zos.putNextEntry(ze);
        	FileInputStream in = new FileInputStream(inputFolder + file);
        	int len;
        	while ((len = in.read(buffer)) > 0) {
        		zos.write(buffer, 0, len);
        	}
        	in.close();
        	zos.closeEntry();
    	}
	}
	
	public static int getZipFileCount(FileSystem fs,final String zipFile)
			throws IOException {
		int filecount=0;
		Path inputPath=new Path(zipFile);
		FSDataInputStream inos=fs.open(inputPath);
		ZipInputStream	zin=new ZipInputStream(inos);
		while ((zin.getNextEntry()) != null) {
			filecount=filecount+1;
		}
		zin.closeEntry();
		zin.close();
		return filecount;
	}
}
