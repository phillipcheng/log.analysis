package etl.cmd;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import bdap.util.Util;
import etl.engine.ETLCmd;
import etl.engine.ProcessMode;
import etl.util.ConfigKey;
import etl.util.ScriptEngineUtil;
import etl.util.SequenceUtil;
import etl.util.VarType;
import scala.Tuple2;

public class SQLCmd extends ETLCmd {
	private static final long serialVersionUID = 1L;

	public static final Logger logger = LogManager.getLogger(SQLCmd.class);

	//cfgkey
	public static final @ConfigKey String cfgkey_incoming_folder = "incoming.folder";
	public static final @ConfigKey String cfgkey_driver = "driver";
	public static final @ConfigKey String cfgkey_url = "url";
	public static final @ConfigKey String cfgkey_user = "user";
	public static final @ConfigKey String cfgkey_password = "password";	
	public static final @ConfigKey String cfgkey_sqls = "sqls";
	public static final @ConfigKey String cfgkey_output_file_prefixs = "output.file.prefixs";

	private String incomingFolder;
	
	public SQLCmd(){
		super();
	}
	
	public SQLCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public SQLCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		String incomingFolderExp = super.getCfgString(cfgkey_incoming_folder, null);
		if (incomingFolderExp!=null){
			this.incomingFolder = (String) ScriptEngineUtil.eval(incomingFolderExp, VarType.STRING, super.getSystemVariables());
			logger.info(String.format("incomingFolder/toFolder:%s", incomingFolder));
		}
	}

	@Override
	public boolean hasReduce(){
		return false;
	}
	
	public static String convertConfiguration(String config){
		if(config==null || config.length()==0) return config;
		
		char[] chars=config.toCharArray();
		for(int i=0;i<chars.length;i++){
			char currChar=chars[i];
			if(currChar==';'){
				if(i==0){
					chars[i]='\n';
				}else if(i>0 && chars[i-1]!='\\'){
					chars[i]='\n';
				}
			}
		}
		
		return new String(chars);
		
	}
	
	@Override
	public Map<String, Object> mapProcess(long offset, String row, 
			Mapper<LongWritable, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos){
		//override param
		Map<String, Object> retMap = new HashMap<String, Object>();		
		List<Tuple2<String, String>> ret=process(offset, row);
		
		for(Tuple2<String,String> item:ret){
			try {
				mos.write(new Text(item._2), null, item._1);
			} catch (Exception e) {
				logger.error("", e);
			}
		}

		List<String> logInfo = new ArrayList<String>();
		logInfo.add(String.valueOf(ret.size()));
		retMap.put(ETLCmd.RESULT_KEY_LOG, logInfo);
		return retMap;
	}
	
	/*
	 * called from sparkProcessFileToKV, key: file Name, v: line value
	 */
	@Override
	public JavaPairRDD<String, String> sparkProcessV2KV(JavaRDD<String> input, JavaSparkContext jsc, Class<? extends InputFormat> inputFormatClass){
		return input.flatMapToPair(new PairFlatMapFunction<String, String, String>(){
			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {
				List<Tuple2<String, String>> ret =process(0, t);
				return ret.iterator();
			}
		});
	}
	
	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String value, Mapper<LongWritable, Text, Text, Text>.Context context) throws Exception {
		super.init();
		List<Tuple2<String, String>> ret= process(0,value);
		return ret;
	}
	
	private List<Tuple2<String, String>> process(long mapKey, String row){
		logger.info(String.format("param: %s", row));
		super.init();	
		
		//Read parameters
		String config=convertConfiguration(row);
		ByteArrayInputStream bis=new ByteArrayInputStream(config.getBytes());		
		PropertiesConfiguration pc=new PropertiesConfiguration();
		try {
			pc.load(bis);
		} catch (ConfigurationException e) {
			logger.warn(String.format("Parse config:%s failed",row), e);
			return new ArrayList<Tuple2<String, String>>();
		}
		
		String driver=pc.getString(cfgkey_driver);
		String url=pc.getString(cfgkey_url);
		String user=pc.getString(cfgkey_user);
		String password=pc.getString(cfgkey_password);
		String[] sqls=pc.getStringArray(cfgkey_sqls);
		String[] outputFilePrefixs=pc.getStringArray(cfgkey_output_file_prefixs);
		logger.info("Execute with driver:{}, url:{}, user:{}, sqls:\n{}, outputFilePrefixs:\n{}", new Object[]{driver, url, user, sqls==null?"":String.join("\n", sqls), outputFilePrefixs==null?"":String.join("\n", outputFilePrefixs)});
		
		if(sqls==null || sqls.length==0) return new ArrayList<Tuple2<String, String>>();
	
		//Execute SQL and extract data			
		List<Tuple2<String, String>> ret=process(driver, url, user, password, sqls, outputFilePrefixs);
		return ret;
		
	}

	private List<Tuple2<String, String>> process(String driver, String url, String user, String password, String[] sqls, String[] outputFilePrefixs) {
		List<Tuple2<String, String>> ret=new ArrayList<Tuple2<String, String>>();
		
		Connection conn;
		try {
			conn = getConnection(driver,url,user,password);
		} catch (Exception e) {
			logger.error("Get connection failed.", e);
			return ret;
		}
		
		Map<String, BufferedWriter> filePrefixWriterMap=new HashMap<String,BufferedWriter>();
		Map<String, String>	filePrefixFileNameMap=new HashMap<String, String>();
		
		for(int i=0;i<sqls.length;i++){
			String sql=sqls[i];
			String outputFilePrefix="default";
			if(outputFilePrefixs!=null && outputFilePrefixs.length>0){
				if(i<outputFilePrefixs.length){
					outputFilePrefix=outputFilePrefixs[i];
				}else{
					outputFilePrefix=outputFilePrefixs[outputFilePrefixs.length-1];
				}
			}
			
			//Create or open HDFS file
			BufferedWriter writer=filePrefixWriterMap.get(outputFilePrefix);
			String fullFilename=filePrefixFileNameMap.get(outputFilePrefix);
			try{
				if(writer==null){
					fullFilename=generateFileName(incomingFolder, outputFilePrefix);
					FSDataOutputStream os=getFs().create(new Path(fullFilename));
					writer=new BufferedWriter(new OutputStreamWriter(os));
					filePrefixWriterMap.put(outputFilePrefix, writer);
					filePrefixFileNameMap.put(outputFilePrefix, fullFilename);
				}
			}catch(Exception e){
				logger.warn("Create/open file failed, , it will skip rest SQL execution.", e);
				break;
			}

			//Extra SQL data
			try {
				extractData(sql, conn, writer);
			} catch (Exception e) {
				logger.warn(String.format("Process SQL:%s failed, it will skip rest SQL execution.", sql), e);
				break;
			}
		}
		
		//Close all writers
		for(String key:filePrefixWriterMap.keySet()){
			BufferedWriter writer=filePrefixWriterMap.get(key);
			if(writer!=null){
				try {
					writer.close();
				} catch (IOException e) {
					logger.warn("Close file failed!", e);
				}
			}
		}
		
		//Close DB connection
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("Close connection failed", e);
			}
		}
		
		//Write filename to output
		for(String key:filePrefixFileNameMap.keySet()){
			logger.info("prefix:{}, filename:{}",key, filePrefixFileNameMap.get(key));
			ret.add(new Tuple2<String, String>(key,filePrefixFileNameMap.get(key)));
		}
		
		return ret;
	}

	private void extractData(String sql, Connection conn, BufferedWriter writer) throws Exception {	
		logger.info("Going to execute SQL:{}", sql );
		
		if(sql.toUpperCase().startsWith("SELECT")){
			ResultSet rs=null;
			try {
				conn.setAutoCommit(false);
				Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				statement.setFetchSize(1000);
				
				rs=statement.executeQuery(sql);
				if(rs!=null){
					int columnCount=rs.getMetaData().getColumnCount();
					while(rs.next()){
						List<String> fields=new ArrayList<String>();
						for(int columnIndex=0;columnIndex<columnCount;columnIndex++){
							fields.add(rs.getString(columnIndex+1));
						}
						String record=Util.getCsv(fields, ",", true, false)+"\n";
						writer.write(record);
						logger.debug("Fetch record:{}", record);
					}
					rs.close();
				}
			} catch (Exception e) {
				if(rs!=null){
					try {
						rs.close();
					} catch (SQLException e1) {
						logger.warn("Fail to close result set.", e1);
						e1.printStackTrace();
					}
				}
				throw e;
			}
			
		}else{
			conn.setAutoCommit(true);
			Statement statement = conn.createStatement();
			int result=statement.executeUpdate(sql);
			writer.write(result+"\n");
			logger.debug("Execute result:{}", result);
		}
	}
	
	private String generateFileName(String root, String fileNamePrefix){
		return String.format("%s/%s_%s_%s", root,fileNamePrefix,System.currentTimeMillis(),SequenceUtil.getSequenceNum(fileNamePrefix));
	}
	
	private Connection getConnection(String driver, String url, String user, String password) throws Exception{
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
	}

	public String getIncomingFolder() {
		return incomingFolder;
	}

	public void setIncomingFolder(String incomingFolder) {
		this.incomingFolder = incomingFolder;
	}
}