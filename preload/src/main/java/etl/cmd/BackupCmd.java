package etl.cmd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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

	private String xmlFolder;
	private String csvFolder;
	private String dataHistoryFolder;
	private String destinationZipFolder;
	private FileSystem fileSys;

	public BackupCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.xmlFolder = pc.getString(BackupCmd.cfgkey_xml_folder);
		this.csvFolder = pc.getString(BackupCmd.cfgkey_csv_folder);
		this.dataHistoryFolder = pc.getString(BackupCmd.cfgkey_data_history_folder);
		this.destinationZipFolder=this.dataHistoryFolder+wfid+".zip";
	}

	@Override
	public List<String> process(String param, Mapper<Object, Text, Text, NullWritable>.Context context) {
		ZipOutputStream zos = null;
		logger.info("param: %"+param);
		Map<String, String> pm = Util.parseMapParams(param);
		if (pm.containsKey(BackupCmd.cfgkey_xml_folder)){
			this.xmlFolder = pm.get(BackupCmd.cfgkey_xml_folder);
		}
		if (pm.containsKey(BackupCmd.cfgkey_csv_folder)){
			this.csvFolder = pm.get(BackupCmd.cfgkey_csv_folder);
		}
		if (pm.containsKey(BackupCmd.cfgkey_data_history_folder)){
			this.dataHistoryFolder = pm.get(BackupCmd.cfgkey_data_history_folder);
		}
		if (pm.containsKey(BackupCmd.cfgkey_destination_zip_folder)){
			this.destinationZipFolder = pm.get(BackupCmd.cfgkey_destination_zip_folder);
		}

		// Code to Zip the xml and csv files.
		try {

			logger.info("Starting Job to Zip files for wfid :  "+wfid);
			String[] dir={xmlFolder,csvFolder};
			fileSys=FileSystem.get(this.getHadoopConf());
			if((!(destinationZipFolder.startsWith(dataHistoryFolder)&&destinationZipFolder.endsWith(".zip"))))
			{
				logger.info("The Destination Zip folder provided is not proper for wfid "+wfid);
				return null;
			}
			Path destpath=new Path(destinationZipFolder);
			FSDataOutputStream fos = fileSys.create(destpath);
			zos = new ZipOutputStream(fos);	
			for (int i = 0; i < dir.length; i++) 
			{
				zipFolder(dir[i],zos);	
			}
			logger.info("Finished Job of Zip files for wfid :  "+wfid);
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

		//Copy and remove xml file code ( existing code )
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
		return null;
	}


	public  void zipFolder(String dirpath, ZipOutputStream zos)
	{

		try {
			Path inputPath = new Path(dirpath);
			fileSys = inputPath.getFileSystem(this.getHadoopConf());
			FileStatus[] status = fileSys.listStatus(inputPath);
			logger.info("Files exist in the folder "+dirpath+": "+(status.length>0));
			for (int i=0;i<status.length;i++){
				Path path =status[i].getPath();
				logger.info("Checking files path:"+path);
				if(status[i].isFile()){
					String fileName=path.getName();
					if(dirpath.equals(xmlFolder) || (fileName.startsWith(wfid, 0)&&(fileName.endsWith(".csv"))))
					{
						logger.info("File Name: "+fileName);   
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

}
