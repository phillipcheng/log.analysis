package bdap.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class HdfsUtil {
	
	public static final Logger logger = LogManager.getLogger(HdfsUtil.class);
	
	public static String getRootPath(String urlPath) {
		if (urlPath != null && urlPath.startsWith("hdfs://")) {
			/* Locate from the root path */
			urlPath = urlPath.substring(7);
			urlPath = urlPath.substring(urlPath.indexOf("/"));
		}
		return urlPath;
	}

	//e.g. hdfs://127.0.0.1:19000
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
	
	public static FileContext getHadoopFsContext(String defaultFs){
		String fs_key = "fs.defaultFS";
		Configuration conf = new Configuration();
		if (defaultFs!=null){
			conf.set(fs_key, defaultFs);
		}
		logger.info(String.format("%s is %s", fs_key, conf.get(fs_key)));
		try {
			return FileContext.getFileContext(conf);
		} catch (IOException e) {
			logger.error("", e);
			return null;
		}
	}
	
	public static String readDfsTextFile(FileSystem fs, String path, int maxFileSize) {
		FSDataInputStream in = null;
		BoundedInputStream boundedIn = null;
		try {
			in = fs.open(new Path(path));
			boundedIn = new BoundedInputStream(in, maxFileSize);
			return IOUtils.toString(boundedIn, StandardCharsets.UTF_8);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			
		} finally {
			if (boundedIn != null)
				try {
					boundedIn.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
		
		return null;
	}
	
	public static String readDfsTextFile(FileSystem fs, String path, long startLine, long endLine) {
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(path));
			LineIterator it = IOUtils.lineIterator(in, StandardCharsets.UTF_8);
			int i = 0;
			StringBuilder buffer = new StringBuilder();
			while (it.hasNext()) {
				if (i >= startLine) {
					if (i < endLine) {
						buffer.append(it.next());
						buffer.append(IOUtils.LINE_SEPARATOR);
					} else {
						break;
					}
				} else {
					it.next();
				}
				i ++;
			}
			return buffer.toString();
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
		
		return null;
	}
	
	public static byte[] readDfsFile(FileSystem fs, String path, int maxFileSize) {
		FSDataInputStream in = null;
		BoundedInputStream boundedIn = null;
		try {
			in = fs.open(new Path(path));
			boundedIn = new BoundedInputStream(in, maxFileSize);
			return IOUtils.toByteArray(boundedIn);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			
		} finally {
			if (boundedIn != null)
				try {
					boundedIn.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
		
		return null;
	}
	
	public static boolean writeDfsFile(FileSystem fs, String path, byte[] content, boolean overwrite){
		FSDataOutputStream out = null;
		try {
			out = fs.create(new Path(path), overwrite);
			out.write(content);
		}catch(Exception e){
			logger.error("",e);
			return false;
		}finally{
			if (out!=null){
				try {
					out.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
		return true;
	}
	
	public static boolean writeDfsFile(FileSystem fs, String path, byte[] content){
		return writeDfsFile(fs, path, content, true);
	}

	public static boolean writeDfsFile(FileSystem fs, String path, InputStream inputStream) {
		FSDataOutputStream out = null;
		try {
			out = fs.create(new Path(path), true);
			IOUtils.copy(inputStream, out);
		}catch(Exception e){
			logger.error("",e);
			return false;
		}finally{
			if (out!=null){
				try {
					out.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
		return true;
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
	
	//used in oozie expression
	public static String getContentsFromDfsFiles(String defaultFs, String filePattern) throws Exception{
		String globFilePattern = filePattern+"*";
		FileSystem fs = getHadoopFs(defaultFs);
		FileStatus[] fstl = fs.globStatus(new Path(globFilePattern));
		List<String> contents = new ArrayList<String>();
		for (FileStatus fst:fstl){
			contents.addAll(stringsFromDfsFile(fs, fst.getPath().toString()));
		}
		return String.join(",", contents);
	}
	
	public static String getContentsFromDfsFilesByPathFilter(String defaultFs, String filePattern,String pathFilter) throws Exception{
		String globFilePattern = filePattern+"*";
		FileSystem fs = getHadoopFs(defaultFs);
		FileStatus[] fstl = fs.globStatus(new Path(globFilePattern));
		List<String> contents = new ArrayList<String>();
		for (FileStatus fst:fstl){
			contents.addAll(stringsFromDfsFilePathFilter(fs, Arrays.asList(new String[]{fst.getPath().toString()}),pathFilter));
		}
		return String.join(",", contents);
	}
	
	public static List<String> stringsFromDfsFile(FileSystem fs, String file){
		return stringsFromDfsFile(fs, Arrays.asList(new String[]{file}));
	}
	
	public static List<String> stringsFromDfsFile(FileSystem fs, List<String> files){
		BufferedReader in = null;
		List<String> sl = new ArrayList<String>();
		for (String file:files){
			try {
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
				String s =null;
				while ((s=in.readLine())!=null){
					sl.add(s);
				}
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
		return sl;
	}
	
	public static List<String> stringsFromDfsFilePathFilter(FileSystem fs, List<String> files,String pathFilter){
		BufferedReader in = null;
		List<String> sl = new ArrayList<String>();
		ExpPathFilter expPathFileter = new ExpPathFilter(pathFilter);
		for (String file:files){
			try {
				in = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
				String s =null;
				while ((s=in.readLine())!=null){
					if(expPathFileter.accept(s)){
						sl.add(s);
					}
				}
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
		return sl;
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
	
	public static List<String> listDfsFilePath(FileSystem fs, String folder, boolean recursive){
		List<String> files = new ArrayList<String>();
		try {
			FileStatus[] fslist = fs.listStatus(new Path(folder));
			for (FileStatus f:fslist){
				if (f.isDirectory())
					files.addAll(listDfsFilePath(fs, f.getPath().toString(), recursive));
				else {
					files.add(f.getPath().toString());
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return files;
	}
	
	public static List<String> listDfsFilePath(FileSystem fs, String folder){
		return listDfsFilePath(fs, folder, false);
	}
	
	public static List<String> listDfsFile(FileSystem fs, String folder){
		List<String> files = new ArrayList<String>();
		try {
			FileStatus[] fslist = fs.listStatus(new Path(folder));
			for (FileStatus f:fslist)
				files.add(f.getPath().getName());
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
	
	public static void toDfsJsonFile(FileSystem fs, String file, Object ls){
		BufferedWriter out = null;
		try{
			out = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(file))));
			out.write(JsonUtil.toJsonString(ls));
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (out!=null){
				try{
					out.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static Object fromDfsJsonFile(FileSystem fs, String file, Class clazz){
		FSDataInputStream fis=null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			fis = fs.open(new Path(file));
			IOUtils.copy(fis, baos);
			String content = baos.toString(JsonUtil.charset);
			return JsonUtil.fromJsonString(content, clazz);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}finally{
			if (fis!=null){
				try{
					fis.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static void checkFolder(FileSystem fs, String folder, boolean clean) throws Exception{
		Path path = new Path(folder);
		if (fs.exists(path) && clean){
			fs.delete(path, true);
		}
		if (!fs.exists(path)){
			fs.mkdirs(path);
			fs.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
		}
	}
}