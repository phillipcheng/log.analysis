package bdap.util;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Util {
	public static final Logger logger = LogManager.getLogger(Util.class);
	
	public static List<int[]> createBatch(int batchSize, int totalSize){
		List<int[]> batches = new ArrayList<int[]>();
		int n = totalSize/batchSize;
		int bn = totalSize%batchSize==0?n:n+1;
		for (int i=0; i<bn; i++){
			if (i!=bn-1){//not last one
				batches.add(new int[]{i*batchSize, (i+1)*batchSize-1});
			}else{
				batches.add(new int[]{i*batchSize, totalSize-1});
			}
		}
		return batches;
	}
	
	public static String getCsv(List<String> csv, boolean newline){
		return getCsv(csv,",",false,newline);
	}
	
	public static String getCsv(List<String> csv, String delimiter, boolean escapingCSV, boolean newline){
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<csv.size(); i++){
			String v = (String) csv.get(i);
			if (v!=null){
				if(escapingCSV==true){
					sb.append(StringEscapeUtils.escapeCsv(v));
				}else{
					sb.append(v);
				}				
			}
			if (i<csv.size()-1){
				sb.append(delimiter);
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
	
	public static FileType guessFileType(String filePath) {
		if (filePath.endsWith(".csv"))
			return FileType.textData;
		else if (filePath.endsWith(".txt"))
			return FileType.textData;
		else if (filePath.endsWith(".properties"))
			return FileType.textData;
		else if (filePath.endsWith(".xml"))
			return FileType.textData;
		else if (filePath.endsWith(".json"))
			return FileType.textData;
		else if (filePath.endsWith(".sql"))
			return FileType.textData;
		else if (filePath.endsWith(".template"))
			return FileType.textData;
		else if (filePath.endsWith(".schema"))
			return FileType.textData;
		else
			return FileType.binaryData;
	}
}
