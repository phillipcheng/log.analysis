package etl.cmd;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.fs.Path;

import etl.util.FieldType;
import etl.util.VarType;

public class CsvFileGenCmd extends SchemaFileETLCmd{
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(CsvFileGenCmd.class);
	
	public static final String TS_FORMAT="yyyy-MM-dd HH:mm:ss.SSS";
	public static final SimpleDateFormat tsSdf = new SimpleDateFormat(TS_FORMAT);
	
	//cfgkey
	public static final String cfgkey_table_names="table.names";
	public static final String cfgkey_output_folder="output.folder";
	public static final String cfgkey_file_size="file.size";
	
	private String[] tableNames;
	private String outputFolder;
	private int fileSize; //unit of lines
	
	private Random rand = new Random();
	
	private String genString(FieldType ft){
		if (VarType.TIMESTAMP==ft.getType()){
			return tsSdf.format(new Date());
		}else if (VarType.STRING==ft.getType()){
			return String.format("string%d", rand.nextInt(10));
		}else if (VarType.INT==ft.getType()){
			return String.format("%d", rand.nextInt(10));
		}else if (VarType.NUMERIC==ft.getType()){
			return String.format("%d.%d", rand.nextInt(10),rand.nextInt(10));
		}else{
			logger.error(String.format("type:%s not supported by genString", ft));
			return null;
		}
	}
	
	public CsvFileGenCmd(){
		super();
	}
	
	public CsvFileGenCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs) {
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public CsvFileGenCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs) {
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs) {
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		this.tableNames = super.getCfgStringArray(cfgkey_table_names);
		this.outputFolder = super.getCfgString(cfgkey_output_folder, null);
		if (this.outputFolder==null){
			logger.error(String.format("output folder not specified."));
		}
		this.fileSize = super.getCfgInt(cfgkey_file_size, 100);
	}
	
	//for each table generate a file with fileSize and put to the output folder
	public List<String> sgProcess(){
		for (String tableName: tableNames){
			List<String> attrNames = logicSchema.getAttrNames(tableName);
			List<FieldType> attrTypes = logicSchema.getAttrTypes(tableName);
			String fileName = String.format("%s_%s_%s", super.getSchemaFileName(), tableName, tsSdf.format(new Date()));
			fileName = fileName.replace(".", "_").replace(" ", "_").replace(":", "_");
			BufferedWriter osw = null;
			try {
				osw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputFolder + fileName))));
				for (int i=0; i<fileSize; i++){//for each line
					StringBuffer sb = new StringBuffer();
					for (int j=0; j<attrNames.size(); j++){//for each field
						FieldType ft = attrTypes.get(j);
						if (j<attrNames.size()-1){
							sb.append(String.format("%s,", genString(ft)));
						}else{
							sb.append(genString(ft));
						}
					}
					osw.write(sb.toString());
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
		return null;
	}

	public String[] getTableNames() {
		return tableNames;
	}

	public void setTableNames(String[] tableNames) {
		this.tableNames = tableNames;
	}

	public String getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}

	public int getFileSize() {
		return fileSize;
	}

	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}
	
}
