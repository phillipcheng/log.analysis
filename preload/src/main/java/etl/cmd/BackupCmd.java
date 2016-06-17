package etl.cmd;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import etl.cmd.dynschema.DynSchemaCmd;
import etl.engine.ETLCmd;

public class BackupCmd extends ETLCmd{
	public static final Logger logger = Logger.getLogger(BackupCmd.class);

	private String xmlFolder;
	private String dataHistoryFolder;
	private String destinationBackupFolder;
	private String destinationZipFolder;
	List<String> fileList;
	public BackupCmd(String wfid, String staticCfg, String inDynCfg, String outDynCfg, String defaultFs){
		super(wfid, staticCfg, inDynCfg, outDynCfg, defaultFs);
		this.xmlFolder = pc.getString(DynSchemaCmd.cfgkey_xml_folder);
		this.dataHistoryFolder = pc.getString(DynSchemaCmd.cfgkey_data_history_folder);
		this.destinationBackupFolder = pc.getString(DynSchemaCmd.cfgkey_backup_history_folder);
		this.destinationZipFolder=this.destinationBackupFolder+".zip";
	}

	@Override
	public List<String> process(String param, Mapper<Object, Text, Text, NullWritable>.Context context) {
		List<String> xmlFiles = dynCfgMap.get(DynSchemaCmd.dynCfg_Key_XML_FILES);
		try {
			for (String xmlFile: xmlFiles){
				FileUtil.copy(fs, new Path(xmlFolder+xmlFile), fs, new Path(dataHistoryFolder+xmlFile), true, this.getHadoopConf());
				logger.info(String.format("copy and remove %s to %s", xmlFolder+xmlFile, dataHistoryFolder+xmlFile));
			}
		}catch(Exception e){
			logger.error("", e);
		}

		// List files and Zip
		try {
			generateFileList(new File(dataHistoryFolder));
			zipFiles(destinationZipFolder);

		} catch (Exception e) {
			// TODO: handle exception
			logger.error("", e);
		}

		return null;
	}

	/**
	 * Zip the files
	 * @param zipFile output ZIP file location
	 */
	public void zipFiles(String zipFile){

		byte[] buffer = new byte[8192];

		try{
			FileSystem fs=FileSystem.get(getHadoopConf());
			Path destpath=new Path(zipFile);
			FSDataOutputStream fos = fs.create(destpath);
			ZipOutputStream zos = new ZipOutputStream(fos);	
			System.out.println("Output to Zip folder : " + zipFile);

			for(String file : this.fileList){

				System.out.println("File Added : " + file);
				ZipEntry ze= new ZipEntry(file);
				zos.putNextEntry(ze);
				Path srcpath=new Path(dataHistoryFolder+ File.separator + file);
				FSDataInputStream in = fs.open(srcpath);
				int len;
				while ((len = in.read(buffer)) > 0) {
					zos.write(buffer, 0, len);
				}

				in.close();
			}

			zos.closeEntry();
			zos.close();

		}catch(IOException ex){
			ex.printStackTrace();   
		}
	}

	/**
	 * Traverse a directory and get all files,
	 * and add the files into fileList  
	 * @param node file or directory
	 */
	public void generateFileList(File node){

		//Add files only
		if(node.isFile()){
			fileList.add(generateZipEntry(node.getAbsoluteFile().toString()));
		}

		if(node.isDirectory()){
			String[] subNote = node.list();
			for(String filename : subNote){
				generateFileList(new File(node, filename));
			}
		}

	}

	/**
	 * Format the file path for zip
	 * @param file file path
	 * @return Formatted file path
	 */
	private String generateZipEntry(String file){
		return file.substring(dataHistoryFolder.length()+1, file.length());
	}


}
