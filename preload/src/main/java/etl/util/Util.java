package etl.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;


public class Util {
	public static final Logger logger = Logger.getLogger(Util.class);
	
	
	public static PropertiesConfiguration getPropertiesConfig(String conf){
		PropertiesConfiguration pc = null;
		try {
			URL url = Thread.currentThread().getContextClassLoader().getResource(conf);
			pc = new PropertiesConfiguration(url);
		} catch (ConfigurationException e) {
			File f = new File(conf);
			try {
				pc = new PropertiesConfiguration(f);
			}catch(Exception e1){
				logger.error("", e1);
			}
		}
		return pc;
	}
	
	public static PropertiesConfiguration getPropertiesConfigFromDfs(FileSystem fs, String conf){
		BufferedReader br = null;
		try {
			Path ip = new Path(conf);
	        br=new BufferedReader(new InputStreamReader(fs.open(ip)));
	        PropertiesConfiguration pc = new PropertiesConfiguration();
	        pc.load(br);
	        return pc;
		}catch(Exception e){
			logger.error("", e);
			return null;
		}finally{
			if (br!=null){
				try{
					br.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	//k1=v1,k2=v2 =>{{k1,v1},{k2,v2}}
	public static TreeMap<String, String> parseMapParams(String params){
		TreeMap<String, String> paramsMap = new TreeMap<String, String>();
		if (params==null){
			return paramsMap;
		}
		String[] strParams = params.split(",");
		for (String strParam:strParams){
			String[] kv = strParam.split("=");
			if (kv.length<2){
				logger.error(String.format("wrong param format: %s", params));
			}else{
				paramsMap.put(kv[0].trim(), kv[1].trim());
			}
		}
		return paramsMap;
	}
	
	public static String getCsv(List<String> csv){
		StringBuffer sb = new StringBuffer();
		for (int i=0; i<csv.size(); i++){
			String v = csv.get(i);
			if (v!=null){
				v = "\"" + v + "\"";//always enclosed by "\""
				sb.append(v);
			}
			if (i<csv.size()-1){
				sb.append(",");
			}
		}
		sb.append("\n");
		return sb.toString();
	}
	
	//json serialization
	public static final String charset="utf8";
	public static Object fromJsonString(String json, Class clazz){
		ObjectMapper mapper = new ObjectMapper();
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		try {
			Object t = mapper.readValue(json, clazz);
			return t;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}

	public static String toJsonString(Object ls){
		ObjectMapper om = new ObjectMapper();
		om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		ObjectWriter ow = om.writer().with(new MinimalPrettyPrinter());
		try {
			String json = ow.writeValueAsString(ls);
			return json;
		} catch (JsonProcessingException e) {
			logger.error("",e );
			return null;
		}
	}
	
	public static void toLocalJsonFile(String file, Object ls){
		PrintWriter out = null;
		try{
			out = new PrintWriter(file, charset);
			out.println(toJsonString(ls));
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (out!=null)
				out.close();
		}
	}
	
	public static void toDfsJsonFile(FileSystem fs, String file, Object ls){
		BufferedWriter out = null;
		try{
			out = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(file))));
			out.write(toJsonString(ls));
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
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			IOUtils.copy(fs.open(new Path(file)), baos);
			String content = baos.toString(charset);
			return fromJsonString(content, clazz);
		}catch(Exception e){
			logger.error("", e);
			return null;
		}
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
		}
	}
	
	//files
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
	
	public static void deleteFiles(String folder, List<String> fileNames) throws IOException {
		for (String fileName : fileNames){
			File file = new File(folder + fileName);
			file.delete();
		}
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
	
	public static void writeDfsFile(FileSystem fs, String fileName, List<String> contents){
		BufferedWriter osw = null;
		try {
			osw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(fileName))));
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
	
	public static void sftpFromLocal(String host, int port, String user, String pass, String localFile, String remoteFile){
		Session session = null;
		ChannelSftp sftpChannel = null;
		try {
			// connect
			JSch jsch = new JSch();
			Channel channel = null;
			session = jsch.getSession(user, host, port);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(pass);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			int slash = remoteFile.lastIndexOf("/");
		    String remotePath = remoteFile.substring(0,slash);
		    String[] folders = remotePath.split( "/" );
		    for ( String folder : folders ) {
		    	logger.info("FolderName:"+folder);
		        if ( folder.length() > 0 ) {
		            try {
		            	sftpChannel.cd( folder );
		            }
		            catch ( SftpException e ) {
		            	sftpChannel.mkdir( folder );
		            	sftpChannel.cd( folder );
		            }
		        }
		    }
		   // sftpChannel.mkdir(remotePath);
			sftpChannel.put(localFile, remoteFile, ChannelSftp.OVERWRITE);
		} catch (Exception e) {
			logger.error("Exception while processing SFTP:", e);
		} finally {

			if (sftpChannel != null) {
				sftpChannel.exit();
			}
			if (session != null) {
				session.disconnect();
			}
		}
	}
	
	public static List<String> sftpList(String host, int port, String user, String pass, String remoteDir){
		Session session = null;
		ChannelSftp sftpChannel = null;
		List<String> fl = new ArrayList<String>();
		try {
			// connect
			JSch jsch = new JSch();
			Channel channel = null;
			session = jsch.getSession(user, host, port);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(pass);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			sftpChannel = (ChannelSftp) channel;
			sftpChannel.cd(remoteDir);
			Vector<LsEntry> v = sftpChannel.ls("*");
			for (LsEntry entry : v) {
				fl.add(entry.getFilename());
			}
		} catch (Exception e) {
			logger.error("Exception while processing SFTP:", e);
		} finally {

			if (sftpChannel != null) {
				sftpChannel.exit();
			}
			if (session != null) {
				session.disconnect();
			}
		}
		return fl;
	}
	
	public static List<String> getMROutput(FileSystem fs, String folder){
		List<String> output = new ArrayList<String>();
		try {
			FileStatus[] fsts = fs.listStatus(new Path(folder));
			if (fsts!=null){
				for (FileStatus fst:fsts){
					BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(fst.getPath())));
					String line = null;
					while ((line=in.readLine())!=null){
						output.add(line);
					}
					in.close();
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return output;
	}
}
