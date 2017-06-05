package etl.cmd;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.CompiledScript;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import bdap.util.HdfsUtil;
import etl.engine.ETLCmd;
import etl.engine.types.DBType;
import etl.engine.types.InputFormatType;
import etl.engine.types.ProcessMode;
import etl.log.LogMarker;
import etl.util.ConfigKey;
import etl.util.DBUtil;
import etl.util.ScriptEngineUtil;
import scala.Tuple2;
import scala.Tuple3;

public class LoadDataCmd extends SchemaETLCmd{
	private static final long serialVersionUID = 1L;
	private static final String NO_TABLE_CONFIGURED = "<no-table-configured>";
	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	public static final Logger logger = LogManager.getLogger(LoadDataCmd.class);
	//cfgkey
	public static final @ConfigKey String cfgkey_webhdfs="hdfs.webhdfs.root";
	public static final @ConfigKey String cfgkey_csvfile = "csv.file";//only for sgprocess
	public static final @ConfigKey String cfgkey_load_sql = "load.sql";
	public static final @ConfigKey(type=String[].class) String cfgkey_table_names="table.names";
	public static final @ConfigKey String cfgkey_dbfile_path="dbfile.path";//for spark to generate dbinput files
	public static final @ConfigKey String cfgkey_delimiter_exp="delimiter.exp";//for spark to generate dbinput files
	//before execute load for each table, it allows user to specify what kind of other sql need to execute in advance. like clean table before load new data.
	public static final @ConfigKey String cfgkey_before_load_table_sqls_exp="before.load.table.sqls.exp";
	//before execute load for each table, it will clean table if specified in the paramater. -ALL- means all table will be cleaned. Only used has table name used.
	public static final @ConfigKey String cfgkey_clean_tables="clean.tables";
	
	public static final @ConfigKey String cfgkey_escape_char="escape.char";	
	public static final @ConfigKey String cfgkey_record_terminator="record.terminator";
	
	//system variables
	public static final String VAR_ROOT_WEB_HDFS="rootWebHdfs";
	public static final String VAR_USERNAME="userName";
	public static final String VAR_CSV_FILE="csvFileName";
	public static final String VAR_TABLE_NAME="tableName";
	public static final String VAR_CSV_FILES="csvFileNames";
	
	private String webhdfsRoot="";
	private String userName;
	//only used in sgprocess
	private String csvFile;
	private transient CompiledScript csCsvFile;
	//
	private String loadSql;
	private transient CompiledScript csLoadSql;
	private transient CompiledScript delimiterCS;

	private String[] tableNames;
	private transient CompiledScript beforeLoadTableSqlsCS=null;
	private List<String> cleanTableNames=new ArrayList<String>();
	
	private String escapeChar=null;
	private String recordTerminator=null;
	
	
	private transient List<String> sgCopySql = new ArrayList<String>();//for sgProcess
	
	public LoadDataCmd(){
		super();
	}
	
	public LoadDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	public LoadDataCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs, ProcessMode pm){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs, pm);
	}
	
	public LoadDataCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, ProcessMode.Single);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs, ProcessMode pm){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs, pm);
		
		this.csvFile = super.getCfgString(cfgkey_csvfile, null);
		logger.info(String.format("csvFile:%s", csvFile));
		this.webhdfsRoot = super.getCfgString(cfgkey_webhdfs, "");
		this.userName = super.getCfgString(DBUtil.key_db_user, null);
		this.loadSql = super.getCfgString(cfgkey_load_sql, null);
		logger.info(String.format("loadSql:%s", loadSql));
		this.tableNames = super.getCfgStringArray(cfgkey_table_names);
		//
		this.getSystemVariables().put(VAR_ROOT_WEB_HDFS, this.webhdfsRoot);
		this.getSystemVariables().put(VAR_USERNAME, this.userName);
		if (this.csvFile!=null){
			csCsvFile = ScriptEngineUtil.compileScript(csvFile);
		}
		if (this.loadSql!=null){
			csLoadSql = ScriptEngineUtil.compileScript(loadSql);
		}
		
		String delimiterExp=super.getCfgString(cfgkey_delimiter_exp, null);
		if(delimiterExp!=null){
			delimiterCS=ScriptEngineUtil.compileScript(delimiterExp);
		}
		
		String beforeLoadTableSqlsExp=super.getCfgString(cfgkey_before_load_table_sqls_exp, null);
		if(beforeLoadTableSqlsExp!=null){
			beforeLoadTableSqlsCS=ScriptEngineUtil.compileScript(beforeLoadTableSqlsExp);
		}
		
		String[] cleanTables=super.getCfgStringArray(cfgkey_clean_tables);
		if(cleanTables!=null){
			cleanTableNames=Arrays.asList(cleanTables);
			logger.info(String.format("cleanTables:%s", String.join(",", cleanTables)));
		}
		
		this.escapeChar=super.getCfgString(cfgkey_escape_char, null);
		logger.info(String.format("escapeChar:%s", escapeChar));
		
		this.recordTerminator=super.getCfgString(cfgkey_record_terminator, null);		
		logger.info(String.format("cfgkey_record_terminator:%s", cfgkey_record_terminator));
	}
	
	private List<String> prepareTableCopySQLs(String tableName, String[] files, String escapeChar, String recordTerminator) {
		
		List<String> newCopysqls = new ArrayList<String>();
		try{
			String sql = null;
			if (logicSchema!=null && (csLoadSql==null||loadSql.contains(VAR_TABLE_NAME))){
				this.getSystemVariables().put(VAR_TABLE_NAME, tableName);
				
				for (String csvFileName: files) {
					csvFileName = HdfsUtil.getRootPath(csvFileName);
					this.getSystemVariables().put(VAR_CSV_FILE, csvFileName);
					
					if (csLoadSql!=null){
						sql = ScriptEngineUtil.eval(csLoadSql, this.getSystemVariables());
					}else{
						String delimiter=",";
						if(delimiterCS!=null){
							delimiter=ScriptEngineUtil.eval(delimiterCS, this.getSystemVariables());
						}
						
						logger.info("Table:{}, Delimiter:{}, Files:\n{}, ",new Object[]{tableName, delimiter, files==null?"":String.join("\n", files)});
						
						sql = DBUtil.genCopyHdfsSql(null, logicSchema.getAttrNames(tableName), tableName, 
								dbPrefix, this.webhdfsRoot, csvFileName, this.userName, this.getDbtype(),delimiter, "\"", this.escapeChar, this.recordTerminator);
					}
					newCopysqls.add(sql);

					logger.info(String.format("sql:%s", sql));
				}
			}else if (csLoadSql!=null){//just evaluate the loadSql
				for (String csvFileName: files) {
					csvFileName = HdfsUtil.getRootPath(csvFileName);
					this.getSystemVariables().put(VAR_CSV_FILE, csvFileName);
					sql = ScriptEngineUtil.eval(csLoadSql, this.getSystemVariables());
					newCopysqls.add(sql);
					
					logger.info(String.format("sql:%s", sql));
				}
			}else{
				logger.error(String.format("wrong combination: logicSchema:%s, loadSql:%s", logicSchema, loadSql));
			}
		}catch(Exception e){
			logger.error("", e);
		}
		return newCopysqls;
	}
	
	@Override
	public List<String> sgProcess() throws Exception{
		List<String> copysqls = new ArrayList<String>();
		if (this.getFs()==null) init();
		List<String> logInfo = new ArrayList<String>();
		try{
			String csvFileName = null;
			String[] files;
			if (logicSchema!=null && (csLoadSql==null||loadSql.contains(VAR_TABLE_NAME))){
				List<String> tryTables = new ArrayList<String>();
				if (tableNames==null || tableNames.length==0){//default sql, match all the files against the tables
					tryTables.addAll(logicSchema.getAttrNameMap().keySet());
				}else{
					tryTables.addAll(Arrays.asList(tableNames));
				}
			
				for (String tableName:tryTables){
					this.getSystemVariables().put(VAR_TABLE_NAME, tableName);
					csvFileName = ScriptEngineUtil.eval(this.csCsvFile, this.getSystemVariables());
					files = new String[] {csvFileName};
					copysqls.addAll(
						prepareTableCopySQLs(tableName, files, this.escapeChar, this.recordTerminator)
					);
				}
			}else{//just evaluate the loadSql
				csvFileName = ScriptEngineUtil.eval(this.csCsvFile, this.getSystemVariables());
				files = new String[] {csvFileName};
				copysqls.addAll(
					prepareTableCopySQLs(null, files,this.escapeChar, this.recordTerminator)
				);
			}
		}catch(Exception e){
			logger.error("", e);
		}
				
		if (super.getDbtype()!=DBType.NONE){
			int rowsAdded = DBUtil.executeSqls(copysqls, super.getPc());
			logInfo.add(rowsAdded+"");
		}
		
		sgCopySql = copysqls;
		
		return  logInfo;
	}
	
	//return table name to file name mapping
	@Override
	public List<Tuple2<String, String>> flatMapToPair(String tableName, String row, Mapper<LongWritable, Text, Text, Text>.Context context){
		logger.info(String.format("in flatMapToPair row:%s", row));
		List<Tuple2<String, String>> vl = new ArrayList<Tuple2<String, String>>();
		row = HdfsUtil.getRootPath(row);
		if (logicSchema!=null && (csLoadSql==null||loadSql.contains(VAR_TABLE_NAME))){
			/* File to table mapping */
			tableName = this.getTableNameSetPathFileName(row);
			if (tableName==null || "".equals(tableName)){
				logger.error(String.format("tfName is empty. exp:%s, value:%s", super.strFileTableMap, row));
			}else{
				vl.add(new Tuple2<String, String>(tableName, row));
			}
		}else{
			vl.add(new Tuple2<String, String>(NO_TABLE_CONFIGURED, row));
		}
		return vl;
	}
	
	private int executeSqls(String key, String[] files) throws Exception{
		super.init();
		if (this.tableNames==null ||tableNames.length==0||Arrays.asList(this.tableNames).contains(key)){
			List<String> sqls = new ArrayList<String>();
			logger.info(String.format("in reduce for key:%s, we need to load file:%s", key.toString(), Arrays.toString(files)));
			
			String beforeLoadTableSQL=generateBeforeLoadTableSQL(key,files);
			
			if(beforeLoadTableSQL!=null && !beforeLoadTableSQL.isEmpty()){
				sqls.add(beforeLoadTableSQL);
			}
			
			if (NO_TABLE_CONFIGURED.equals(key.toString())){
				sqls.addAll(prepareTableCopySQLs(null, files,this.escapeChar, this.recordTerminator));
			} else {
				if(cleanTableNames.contains("-ALL-") || cleanTableNames.contains(key)){
					sqls.add(generateCleanTableSQL(key));
				}
				sqls.addAll(prepareTableCopySQLs(key.toString(), files,this.escapeChar, this.recordTerminator));
			}			
			
			if (super.getDbtype()!=DBType.NONE){
				logger.info("Following SQLs would be executed:\n{}", String.join("\n", sqls));
				return DBUtil.executeSqls(sqls, super.getPc());
			}
		}else{
			logger.warn(String.format("skip loading table:%s, tableNames:%s", key, Arrays.asList(this.tableNames)));
		}
		return 0;
	}
	
	private String generateCleanTableSQL(String tableName){
		return String.format("truncate table %s.%s;", dbPrefix,tableName);
	}
	
	private String generateBeforeLoadTableSQL(String key, String[] files){
		String sql=null;		
		if(beforeLoadTableSqlsCS!=null){
			Map<String,Object> variables=new HashMap<String,Object>();
			variables.putAll(this.getSystemVariables());
			variables.put(VAR_TABLE_NAME, key);
			variables.put(VAR_CSV_FILES, files);
			sql=ScriptEngineUtil.eval(this.beforeLoadTableSqlsCS, variables);
			logger.info("Generate before load table SQL:{}", sql);
		}		
		return sql;
	}
	
	@Override
	public List<Tuple3<String,String,String>> reduceByKey(String key, Iterable<? extends Object> values,
			Reducer<Text, Text, Text, Text>.Context context, MultipleOutputs<Text, Text> mos) throws Exception {
		String[] files = itToSet(values);
		Date startdt=new Date();
		int rows = executeSqls(key.toString(), files);
		Date enddt=new Date();
		
		//AUDIT how many rows added.
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		logger.info(LogMarker.AUDIT.marker, String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s", sdf.format(startdt),sdf.format(enddt),wfName,wfid,getClass().getName(), rows, "","",""));
		
		List<Tuple3<String,String,String>> ret = new ArrayList<Tuple3<String,String,String>>();
		ret.add(new Tuple3<String,String,String>(key.toString(), Integer.toString(rows), ETLCmd.SINGLE_TABLE));
		return ret;
	}
	
	/**
	 * @param input: tuple of tableName and one line of content
	 * @param jsc
	 * @return
	 */
	@Override
	public JavaPairRDD<String, String> sparkProcessKeyValue(JavaPairRDD<String, String> input, JavaSparkContext jsc, 
			InputFormatType ift, SparkSession spark){
		return input.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
				init();
				String[] files = StringItToFiles(t._2);
				String tfName = null;
				if (ift!=null){
					tfName = getTableNameSetPathFileName(t._1.toString());
				}else{//processed
					tfName = t._1.toString();
				}
				if (tfName!=null && !"".equals(tfName)){
					int rows = executeSqls(tfName, files);
					return new Tuple2<String, String>(tfName, Integer.toString(rows));
				}else{
					logger.warn(String.format("get tfname is null: key:%s, exp:%s", t._1.toString(), strFileTableMap));
					return null;
				}
			}
		});
	}

	private String[] itToSet(Iterable<? extends Object> values) {
		Set<String> files = new HashSet<String>();
		Iterator<? extends Object> it = values.iterator();
		while (it.hasNext()) {
			String v = it.next().toString();
			files.add(v);
		}
		return files.toArray(EMPTY_STRING_ARRAY);
	}
	
	private String[] StringItToFiles(Iterable<String> values) {
		Set<String> files = new HashSet<String>();
		Iterator<String> it = values.iterator();
		while (it.hasNext()) {
			String v = it.next().toString();
			files.add(v);
		}
		return files.toArray(EMPTY_STRING_ARRAY);
	}

	public boolean hasReduce() {
		return true;
	}

	public List<String> getSgCopySql() {
		return sgCopySql;
	}

	public void setSgCopySql(List<String> sgCopySql) {
		this.sgCopySql = sgCopySql;
	}
}