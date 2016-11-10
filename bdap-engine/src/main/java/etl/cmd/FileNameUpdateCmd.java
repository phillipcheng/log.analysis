package etl.cmd;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import etl.engine.ETLCmd;
import etl.util.ScriptEngineUtil;
import etl.util.VarType;

public class FileNameUpdateCmd extends ETLCmd {

	private static final long serialVersionUID = -2221322342512974142L;	
	public static final Logger logger = LogManager.getLogger(FileNameUpdateCmd.class);
	
	private static final int blockSize=1024;
	
	//cfgkey
	public static final String cfgkey_incoming_folder="incomingFolder";
	public static final String cfgkey_file_name="file.name";
	public static final String cfgkey_headlines="headlines";
	public static final String cfgkey_taillines="taillines";
	
	//var name
	public static final String VAR_NAME_HEADLINES="headlines";
	public static final String VAR_NAME_TAILLINES="taillines";
	
	private String incomingFolder;
	private String fileNameExp;
	private int headlines;
	private int taillines;
	
	public FileNameUpdateCmd(){
		super();
	}
	
	public FileNameUpdateCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public FileNameUpdateCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		incomingFolder =super.getCfgString(cfgkey_incoming_folder, null);
		fileNameExp =super.getCfgString(cfgkey_file_name, null);
		headlines=super.getCfgInt(cfgkey_headlines, 10);
		taillines=super.getCfgInt(cfgkey_taillines, 10);
		
		if(headlines<0) headlines=0;
		if(taillines<0) taillines=0;
	}
	
	@Override
	public List<String> sgProcess() {
		if(incomingFolder==null){
			logger.error("The incoming folder is not assigned");
			return null;
		}
		
		Path incomingFolderPath=new Path(incomingFolder);
		FileStatus[] fileStatusArray;
		try {
			fileStatusArray = fs.listStatus(incomingFolderPath);
		} catch (FileNotFoundException e) {
			logger.error("The incoming folder doesn't exist:",e);
			return null;
		} catch (IOException e) {
			logger.error("Cannot open incoming folder:",e);
			return null;
		}
		
		List<Path> paths = new ArrayList<Path>();
		for(FileStatus filestatus:fileStatusArray){
			if(!filestatus.isFile()){
				continue;
			}
			paths.add(filestatus.getPath());			
		}
		
		for(Path path:paths){
			updateFileName(path);			
		}
		return null;
	}

	@Override
	public Map<String, Object> mapProcess(long offset, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		Path path=new Path(row);
		updateFileName(path);
		return null;
	}
	
	private void updateFileName(Path path) {
		List<String> headlinesList=new ArrayList<String>();
		List<String> taillinesList=new ArrayList<String>();
		
		if(readHeadTailLines(path, headlinesList, taillinesList)){
			logger.debug("headlines:{}", headlinesList);
			logger.debug("taillines:{}", taillinesList);
		}
		
		super.getSystemVariables().put(VAR_NAME_HEADLINES, headlinesList.toArray(new String[0]));
		super.getSystemVariables().put(VAR_NAME_TAILLINES, headlinesList.toArray(new String[0]));
		super.getSystemVariables().put(VAR_NAME_FILE_NAME, path.getName());
		
		
		
		String fileName = (String) ScriptEngineUtil.eval(fileNameExp, VarType.STRING, super.getSystemVariables());
		
		fileName=path.getParent().toString()+"/"+fileName;
		logger.info("Will rename to {}", fileName);
		
		try {
			fs.rename(path, new Path(fileName));
		}catch (Exception e) {
			logger.error("Rename failed",e);
		}
	}

	private boolean readHeadTailLines(Path path, List<String> headlinesList, List<String> taillinesList) {
		logger.info("Start to read {} head & tail lines.", path.getName());
		FSDataInputStream fsDataInputStream=null;
		BufferedReader br=null;
		try{
			fsDataInputStream=fs.open(path);
			br=new BufferedReader(new InputStreamReader(fsDataInputStream));
			
			//Read head lines
			if(headlines>0){			        
				String line=br.readLine();
			    for(int i=0;i<headlines && line!=null;i++){
			    	headlinesList.add(line);
			        line=br.readLine();
			    }
			}
					
			
			/*
			 * Read tail lines:
			 * read a block since tail with offset and combine with previous read block
			 * transfer into string and count lines
			 * if lines<=tail lines number and offset position=0: return lines
			 * if lines<=tail lines number  and offset position>0: continue
			 * if lines>tail lines number : return lines with tail lines number 
			 */
			
			if(taillines==0) return true;
			
			byte[] allBlocks=new byte[0];
			byte[] blockBuffer=new byte[blockSize];
			long position=fs.getFileStatus(path).getLen() -blockSize;
			if(position<0) position=0;
			boolean continueFlag=true;
			do{					
				int readSize=fsDataInputStream.read(position, blockBuffer, 0, blockSize);
				if(readSize>0){
					byte[] oldAllBlocks=allBlocks;
					allBlocks=new byte[readSize+allBlocks.length];						
					System.arraycopy(blockBuffer, 0, allBlocks, 0, readSize);
					if(oldAllBlocks.length>0){
						System.arraycopy(oldAllBlocks, 0, allBlocks, readSize, oldAllBlocks.length);
					}					
				}
				
				BufferedReader lbr = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(allBlocks)));
				List<String> lines=new ArrayList<String>();
				String line = lbr.readLine();
				while (line != null) {
					lines.add(line);
					line = lbr.readLine();
				}
				lbr.close();

				if (lines.size() > taillines) {
					for(int i=0;i<taillines;i++){
						taillinesList.add(lines.get(lines.size()-i-1));
					}
					continueFlag = false;
				} else if (lines.size() <=taillines && position == 0) {
					Collections.reverse(lines);
					taillinesList.addAll(lines);
					continueFlag = false;
				} else if (lines.size() <=taillines && position > 0) {
					position = position - blockSize;
					if (position < 0) position = 0;
				}						
			}while (continueFlag);
			
			return true;
		} catch (IOException e){
			logger.warn("Failed to read head & tail line.",e);
			return false;
		} finally{
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if(fsDataInputStream!=null){
				try {
					fsDataInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
}
