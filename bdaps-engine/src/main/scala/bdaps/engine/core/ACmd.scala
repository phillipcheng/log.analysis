package bdaps.engine.core

import javax.script.CompiledScript

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.commons.csv._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import etl.engine.EngineUtil
import etl.engine.ETLCmd
import etl.engine.types.InputFormatType
import etl.input._
import etl.util.ScriptEngineUtil
import etl.util.SchemaUtils
import etl.input.CombineWithFileNameTextInputFormat
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ACmdConst{
	val cfgkey_skip_header = "skip.header";
	val cfgkey_skip_header_exp="skip.header.exp";
	val cfgkey_file_table_map="file.table.map";
	val cfgkey_key_value_sep="key.value.sep";//key value sep
	val cfgkey_csv_value_sep="input.delimiter";//csv value sep
	val cfgkey_input_endwith_delimiter="input.endwithcomma";//ending delimiter
	
	//system variables
	val VAR_NAME_TABLE_NAME="tablename";
	val VAR_NAME_FILE_NAME="filename";
	val VAR_NAME_PATH_NAME="pathname";//including filename
	val VAR_WFID="WFID"; //string type 
	val VAR_FIELDS="fields"; //string[] type
	
	val DEFAULT_KEY_VALUE_SEP="\t";
	val DEFAULT_CSV_VALUE_SEP=",";
	val SINGLE_TABLE="singleTable";
	val COLUMN_PREFIX="c";
}

class ACmd(val wfName:String, val wfid:String, val staticCfg:String, val prefix:String, val defaultFs:String) extends Serializable{
	var logger : Logger =null
	@transient
	var pc : PropertiesConfiguration = null
	@transient
	var conf: Configuration = null
	@transient
	var fs:FileSystem=null
	var skipHeader:Boolean = false
	@transient
	var skipHeaderCS:CompiledScript = null
	var csvValueSep:String = null
	var keyValueSep:String = null
	var inputEndwithDelimiter:Boolean = false;
	var systemVariables:java.util.Map[String, Object] = null
	
	def reinit{
	  if (pc==null)
	    init
	}
	
	def init {
		if (pc==null){ 
		  logger = LoggerFactory.getLogger(classOf[ACmd])
		  pc = EngineUtil.getInstance.getMergedPC(staticCfg)
		  val fs_key = "fs.defaultFS";
			conf = new Configuration();
			if (defaultFs!=null){
				conf.set(fs_key, defaultFs);
			}
			this.fs = FileSystem.get(conf);
			csvValueSep=pc.getString(ACmdConst.cfgkey_csv_value_sep, ACmdConst.DEFAULT_CSV_VALUE_SEP);
			keyValueSep=pc.getString(ACmdConst.cfgkey_key_value_sep, ACmdConst.DEFAULT_KEY_VALUE_SEP);
			inputEndwithDelimiter=pc.getBoolean(ACmdConst.cfgkey_input_endwith_delimiter, false);
			skipHeader =pc.getBoolean(ACmdConst.cfgkey_skip_header, false);
		  val skipHeaderExpStr = pc.getString(ACmdConst.cfgkey_skip_header_exp, null);
		  if (skipHeaderExpStr!=null){
		    skipHeaderCS = ScriptEngineUtil.compileScript(skipHeaderExpStr);
		  }
			systemVariables = new java.util.HashMap[String, Object]()
			systemVariables.put(ACmdConst.VAR_WFID, wfid);
			copyConf
    }
  }

/******
 * Overridable methods
 */
  def hasReduce():Boolean = {true}
  
  //will be overriden by schema-etl-cmd
  def mapKey(key:String):String = {
    return key;
  }
  
  def useSparkSql():Boolean = {false}
  
  //
  def dataSetProcess(spark:SparkSession, singleTableColNum:Int):RDD[(String,String)] = {
		logger.error(String.format("Empty dataSetProcess impl!!! %s", this));
		return null;
	}
	
	//map method sub class needs to implement
  @throws(classOf[Exception])
	def flatMapToPair(tableName:String, value:String):List[(String,String)] = {
		logger.error(String.format("Empty flatMapToPair impl!!! %s", this));
		return null;
	}
	
	//reduce method sub class needs to implement //k,v,baseoutput
	@throws(classOf[Exception])
	def reduceByKey(key:String, it:Iterable[String]):List[(String,String,String)] = {
		logger.error(String.format("Empty reduceByKey impl!!! %s", this));
		return null;
	}
  
  //files to KV
  def sparkProcessFilesToKV(inputfiles:RDD[String], ift:InputFormatType, spark:SparkSession) : RDD[(String,String)] = {
    var jobConf : JobConf = new JobConf(conf);
    inputfiles.collect().map(s=>FileInputFormat.addInputPath(jobConf, new Path(s))) //add input paths to job conf
    var realInputFormatClass = ETLCmd.getInputFormat(ift)
    var realIft = ift;
    if (InputFormatType.Text==ift){//for text input format, we need to record file name
      realInputFormatClass = classOf[CombineWithFileNameTextInputFormat]
      realIft = InputFormatType.CombineWithFileNameText
    }
    var rdd:RDD[(LongWritable,Text)] = spark.sparkContext.newAPIHadoopRDD(jobConf, realInputFormatClass, classOf[LongWritable], classOf[Text])
    if (skipHeader){
      rdd = rdd.filter(x=> if (x._1.get()==0) false else true)
    }
    return sparkProcessKeyValue(rdd.flatMap(x=>{
      reinit
      preprocess((null, x._2.toString()),realIft)
      }), null, spark)
  }
  
  //KV to KV
	def sparkProcessKeyValue(input:RDD[(String,String)], ift:InputFormatType, spark:SparkSession): RDD[(String,String)]= {
		var pprdd:RDD[(String,String)] = input;
		if (ift != null){
		  pprdd = input.flatMap(x=>{preprocess(x, ift)})
		}
		if (useSparkSql()){
		  if (this.isInstanceOf[SchemaCmd]){
		    var colNum=0;
		    val scmd:SchemaCmd = this.asInstanceOf[SchemaCmd]
		    if (scmd.getLogicSchema() == null){//schema less, each field is treated as string typed
		      colNum = pprdd.values.top(1).apply(0).split(csvValueSep, -1).length
					val fields = (0 until colNum).map(i=> StructField(ACmdConst.COLUMN_PREFIX+i, DataTypes.StringType, true))
					val schema = StructType(fields)
					val delimiter:Char= csvValueSep.charAt(0);
					val rowRDD:RDD[Row]= pprdd.values.map(record=>{
							val csvParser:CSVParser=CSVParser.parse(record, CSVFormat.DEFAULT.withTrim().
									withDelimiter(delimiter).withTrailingDelimiter(inputEndwithDelimiter).
									withSkipHeaderRecord(skipHeader));
							val csvRecord:CSVRecord = csvParser.getRecords().get(0);
							val fvs = (0 until csvRecord.size()).map(i=>csvRecord.get(i))
						  Row.fromSeq(fvs);
						});
					val dfr = spark.createDataFrame(rowRDD, schema);
					dfr.createOrReplaceTempView(ACmdConst.SINGLE_TABLE);
				}else{
					val keys = pprdd.keys.distinct().collect();
					keys.map(key=>{
					  reinit
					  val values = pprdd.filter(x=> x._1==key).map(x=>x._2);
					  val st = scmd.getSparkSqlSchema(key);
					  if (st!=null){
					    val rowRDD = values.map(record=>{
									val attributes = SchemaUtils.convertFromStringValues(
											scmd.getLogicSchema(), key, record, csvValueSep, 
											inputEndwithDelimiter, skipHeader);
									Row.fromSeq(attributes)
								}
							);
							val dfr = spark.createDataFrame(rowRDD, st);
							dfr.createOrReplaceTempView(key);
					  }else{
					    logger.error(String.format("schema not found for %s", key));
					  }
					});
				}
		    return dataSetProcess(spark, colNum);
		  }else{
		    logger.error(String.format("expected SchemaETLCmd subclasses. %s", this.getClass().getName()));
				return null;
		  }
		}else{
		  val csvgroup = pprdd.flatMap(t=>{
		    reinit;
		    flatMapToPair(t._1, t._2);
		  });
			if (hasReduce()){
				csvgroup.groupByKey().flatMap(t=>{
				  if (t!=null){
						val t3l = reduceByKey(t._1, t._2);
						t3l.map(t3=>{
						  if (t3._2!=null)
						    (t3._3, t3._1 + csvValueSep + t3._2)
						  else
						    (t3._3, t3._1)
						});
					}else{
					  List()
					}
				});
			}else{
				return csvgroup;
			}
		}
	}
	
/*****
 * Utility functions
 */
	//copy propertiesConf to jobConf
	def copyConf {
		val it : java.util.Iterator[_] = pc.getKeys
		while (it.hasNext()){
			var key:String = it.next().asInstanceOf[String]
			var value = pc.getString(key);
			this.conf.set(key, value);
		}
	}
	
  def processSequenceFile(pathName:String, content:String):List[(String, String)] = {
    var pname = pathName;
		val lines = content.split("\n");
		if (pathName==null){
			pname = lines(0);
		}
		val key = mapKey(pname);
		var skipFirstLine = skipHeader;
		if (!skipHeader && skipHeaderCS!=null){
			val skip = ScriptEngineUtil.evalObject(skipHeaderCS, systemVariables).asInstanceOf[Boolean];
			skipFirstLine = skipFirstLine || skip;
		}
		var start=1;
		if (skipFirstLine) start=start+1;
		val effLines = lines.slice(start, lines.size)
		return List.fill(effLines.size)(key).zip(effLines)
	}
  
  def preprocess(inkv:(String,String), ift:InputFormatType):List[(String, String)] = {
    if (InputFormatType.SequenceFile==ift){
      return processSequenceFile(null, inkv._2)
    }else {
      if (ift == InputFormatType.CombineWithFileNameText){
        val kv = inkv._2.split(CombineWithFileNameTextInputFormat.filename_value_sep, 2);
        List((mapKey(kv(0)),kv(1)))
      }else if (ift == InputFormatType.FileName){
        List((mapKey(inkv._2), inkv._2))
      }else if (ift == InputFormatType.Text){
        List((mapKey(inkv._1), inkv._2))
      }else{
        logger.error(String.format("input format not supported:%s", ift))
        return null
      }
    }
  }
}