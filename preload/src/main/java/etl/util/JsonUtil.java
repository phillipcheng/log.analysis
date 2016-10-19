package etl.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class JsonUtil {
	public static final Logger logger = LogManager.getLogger(JsonUtil.class);
	//json serialization
	public static final String charset="utf8";
	
	public static <T> T fromJsonString(String json, Class<T> clazz){
		return fromJsonString(json, clazz, false);
	}
	
	public static String toJsonString(Object ls){
		return toJsonString(ls, false);
	}
	
	public static <T> T fromJsonString(String json, Class<T> clazz, boolean useDefaultTyping){
		ObjectMapper mapper = new ObjectMapper();
		if (useDefaultTyping){
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		}
		mapper.setSerializationInclusion(Include.NON_NULL);
		try {
			return mapper.readValue(json, clazz);
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	public static String toJsonString(Object ls, boolean useDefaultTyping){
		ObjectMapper mapper = new ObjectMapper();
		if (useDefaultTyping){
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		}
		mapper.setSerializationInclusion(Include.NON_NULL);
		ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
		try {
			String json = ow.writeValueAsString(ls);
			return json;
		} catch (JsonProcessingException e) {
			logger.error("",e );
			return null;
		}
	}
	
	//from use default typing to not using default typing
	public static void migrateJson(String infile, String outfile, Class clazz){
		java.nio.file.Path path = java.nio.file.FileSystems.getDefault().getPath(infile);
		java.nio.file.Path outpath = java.nio.file.FileSystems.getDefault().getPath(outfile);
		try {
			String contents = new String(Files.readAllBytes(path));
			Object obj = fromJsonString(contents, clazz, true);
			if (obj!=null){
				String newContents = toJsonString(obj, false);
				Files.write(outpath, newContents.getBytes());
			}else{
				logger.error(String.format("error reading infile:%s", infile));
			}
		}catch(Exception e){
			logger.error("", e);
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
	
	public static Object fromLocalJsonFile(String file, Class clazz){
		java.nio.file.Path path = java.nio.file.FileSystems.getDefault().getPath(file);
		try {
			String contents = new String(Files.readAllBytes(path));
			return fromJsonString(contents, clazz);
		}catch(Exception e){
			logger.error("", e);
			return null;
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
		FSDataInputStream fis=null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			fis = fs.open(new Path(file));
			IOUtils.copy(fis, baos);
			String content = baos.toString(charset);
			return fromJsonString(content, clazz);
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
}
