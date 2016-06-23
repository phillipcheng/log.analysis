package etl.cmd;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.engine.ETLCmd;
import etl.util.Util;

public class BackupCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);

	public static final String cfgkey_xml_folder="xml-folder";
	public static final String cfgkey_csv_folder="csv-folder";
	public static final String cfgkey_data_history_folder="data-history-folder";
	public static final String cfgkey_destination_zip_folder="destination-zip-folder";
	public static final String cfgkey_Folder_filter="folder.filter";
	public static final String cfgkey_file_filter="file.filter";
	public static final String defaultFolderFilter="defaultFolder.filter";

	private String xmlFolder;
	private String csvFolder;
	private String dataHistoryFolder;
	private String destinationZipFolder;
	private FileSystem fileSys;
	private String inDynCfg;
	private String[] folderFilter;
	private String[] fileFilter;
	private String userFilter;
	private ZipOutputStream zos;

	public BackupCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);

		this.dataHistoryFolder = pc.getString(cfgkey_data_history_folder);
		this.folderFilter = pc.getStringArray(cfgkey_Folder_filter);
		this.fileFilter = pc.getStringArray(cfgkey_file_filter);
		this.destinationZipFolder=this.dataHistoryFolder+wfid+".zip";
		this.xmlFolder = folderFilter[0];
		this.csvFolder = folderFilter[1];
		this.userFilter=fileFilter[0];
		this.inDynCfg=inDynCfg;
	}

	@Override
	public List<String> process(String param, Mapper<Object, Text, Text, NullWritable>.Context context) {

		logger.info("param: "+param);

		Map<String, String> pm = Util.parseMapParams(param);
		if (pm.containsKey(cfgkey_xml_folder)){
			this.xmlFolder = pm.get(cfgkey_xml_folder);
		}
		if (pm.containsKey(cfgkey_csv_folder)){
			this.csvFolder = pm.get(cfgkey_csv_folder);
		}
		if (pm.containsKey(cfgkey_data_history_folder)){
			this.dataHistoryFolder = pm.get(cfgkey_data_history_folder);
		}
		if (pm.containsKey(cfgkey_destination_zip_folder)){
			this.destinationZipFolder = pm.get(cfgkey_destination_zip_folder);
		}
		if (pm.containsKey(cfgkey_Folder_filter)){

			this.folderFilter=pm.get(cfgkey_Folder_filter).split(" ");
		}
		if (pm.containsKey(cfgkey_file_filter)){
			this.userFilter = pm.get(cfgkey_file_filter);
		}

		try {
			fileSys=FileSystem.get(this.getHadoopConf());
			if((!(destinationZipFolder.startsWith(dataHistoryFolder)&&destinationZipFolder.endsWith(".zip"))))
			{
				logger.info("The Destination Zip folder provided is not valid for wfid "+wfid);
				return null;
			}
			Path destpath=new Path(destinationZipFolder);
			FSDataOutputStream fos = fileSys.create(destpath);
			zos = new ZipOutputStream(fos);	
			for (int i = 0; i < folderFilter.length; i++) 
			{
				zipFolder(folderFilter[i]);	
			}
			logger.info("Finished Job :  "+wfid);
		} 
		catch (NullPointerException e) {
			logger.error("Null Pointer exception ...!  ", e);
		}
		catch (IOException e) {
			logger.error("IO Exception...!  ", e);
		}
		catch (Exception e) {
			logger.error(" ", e);
		}
		finally 
		{
			try 
			{  // closed safely 
				zos.close();
			} 
			catch(IOException e) {
				logger.error("Exception closing IO streams ...! ", e);
			}
		}

		//	Copy and remove xml file code ( existing code )
		if(inDynCfg!=null)
		{
			logger.error("Dynamic Cfg is not null ...! ");
			List<String> xmlFiles = dynCfgMap.get(DynSchemaCmd.dynCfg_Key_XML_FILES);
			try {
				for (String xmlFile: xmlFiles){
					FileUtil.copy(fs, new Path(xmlFolder+xmlFile), fs, new Path(dataHistoryFolder+xmlFile), true, this.getHadoopConf());
					logger.info(String.format("copy and remove %s to %s", xmlFolder+xmlFile, dataHistoryFolder+xmlFile));

				}
			}

			catch(Exception e){
				logger.error("", e);
			}	
		}

		return null;
	}

	public  void zipFolder(String dirpath)
	{
		try {
			Path inputPath = new Path(dirpath);
			fileSys = inputPath.getFileSystem(this.getHadoopConf());
			FileStatus[] status = fileSys.listStatus(inputPath);
			for (int i=0;i<status.length;i++){
				Path path =status[i].getPath();
				if(status[i].isFile()){
					String fileName=path.getName();
					if(dirpath.equals(xmlFolder)&&(userFilter.equals(fileFilter[0])||userFilter.equals(fileFilter[2])))
					{
						zipFile(dirpath,fileName);
					}
					else if(dirpath.equals(csvFolder)&&(userFilter.equals(fileFilter[1])||userFilter.equals(fileFilter[2])))
					{
						if(fileName.startsWith(wfid, 0))
						{
							zipFile(dirpath,fileName);
						}
					}

					else if(userFilter.equals(fileFilter[2]))
					{
						zipFile(dirpath,fileName);	
					}

				}
			}
		}
		catch(FileNotFoundException e)
		{
			logger.error("Folder does not exists .! Exception occured...! ", e);
		}
		catch (NullPointerException e) {
			logger.error("Null Pointer exception ...!  ", e);
		}
		catch (IOException e) {
			// TODO: handle exception
			logger.error("File IO exception occured...! ", e);
		}
		catch (Exception e) {
			// TODO: handle exception
			logger.error(" ", e);
		}
	}

	public void zipFile(String dirpath,String fileName)
	{

		try {
			logger.info("Adding file "+fileName); 
			ZipEntry ze= new ZipEntry(fileName);
			zos.putNextEntry(ze);
			Path srcpath=new Path(dirpath+ File.separator + fileName);
			logger.info("src path is "+srcpath);
			FSDataInputStream in = fileSys.open(srcpath);
			int buffersize =8192;
			byte[] buffer = new byte[buffersize];
			int count;
			while((count = in.read(buffer)) != -1)
			{
				zos.write(buffer,0,count);    
			}
			zos.closeEntry();
			in.close();

		} 	catch(FileNotFoundException e)
		{
			logger.error("Folder does not exists .! Exception occured...! ", e);
		}
		catch (NullPointerException e) {
			logger.error("Null Pointer exception ...!  ", e);
		}
		catch (IOException e) {
			// TODO: handle exception
			logger.error("File IO exception occured...! ", e);
		}
		catch (Exception e) {
			// TODO: handle exception
			logger.error(" ", e);
		}

	}

}
