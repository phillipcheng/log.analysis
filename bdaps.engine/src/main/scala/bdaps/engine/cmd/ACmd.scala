package bdaps.engine.cmd

import javax.script.CompiledScript;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.configuration.PropertiesConfiguration

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import etl.engine.EngineUtil
import etl.input._
import etl.util.ScriptEngineUtil

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
	val logger = LogManager.getLogger(classOf[ACmd])
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
	//copy propertiesConf to jobConf
	def copyConf {
		val it : java.util.Iterator[_] = pc.getKeys
		while (it.hasNext()){
			var key:String = it.next().asInstanceOf[String]
			var value = pc.getString(key);
			logger.debug(String.format("copy property:%s:%s", key, value));
			this.conf.set(key, value);
		}
	}
	
	def init {
		if (pc==null){
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
			skipHeader =pc.getBoolean(ACmdConst.cfgkey_skip_header);
		  val skipHeaderExpStr = pc.getString(ACmdConst.cfgkey_skip_header_exp, null);
		  if (skipHeaderExpStr!=null){
		    skipHeaderCS = ScriptEngineUtil.compileScript(skipHeaderExpStr);
		  }
			systemVariables = new java.util.HashMap[String, Object]()
			systemVariables.put(ACmdConst.VAR_WFID, wfid);
			copyConf
    }
  }
  
  //will be overriden by schema-etl-cmd
  def mapKey(key:String):String = {
    return key;
  }
  
  def processSequenceFile(pathName:String, content:String):List[Tuple2[String, String]] = {
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
		var ret:List[Tuple2[String,String]] = List[Tuple2[String,String]]();
		lines.map(s=> ret=(key, s)::ret);
		return ret;
	}
  
  //files to KV
  def sparkProcessFilesToKV(inputfiles:RDD[String],  
			inputFormatClass:Class[_ <: InputFormat[LongWritable,Text]], spark:SparkSession) : RDD[Tuple2[String,String]] = {
    var jobConf : JobConf = new JobConf(conf);
    inputfiles.collect().map(s=>FileInputFormat.addInputPath(jobConf, new Path(s))) //add input paths to job conf
    var realInputFormatClass = inputFormatClass
    if (inputFormatClass.isAssignableFrom(classOf[TextInputFormat])){//for text input format, we need to record file name
      realInputFormatClass = classOf[CombineWithFileNameTextInputFormat]
    }
    val rdd = spark.sparkContext.newAPIHadoopRDD(jobConf, realInputFormatClass, classOf[LongWritable], classOf[Text])
    var processedInput:RDD[Tuple2[String,String]]=null
    if (realInputFormatClass.isAssignableFrom(classOf[SequenceFileInputFormat[LongWritable, Text]])){
      processedInput = rdd.flatMap(x=>processSequenceFile(null, x._2.toString()))
    }else {
      processedInput = rdd.map(x=>{
        var key:String = null;
        var value:String = x._2.toString();
        if (realInputFormatClass.isAssignableFrom(classOf[CombineWithFileNameTextInputFormat])){
          val kv = x._2.toString().split(CombineWithFileNameTextInputFormat.filename_value_sep, 2);
          key = kv(0)
          value=kv(1)
        }else if (realInputFormatClass.isAssignableFrom(classOf[FilenameInputFormat])){
          key = x._2.toString();
			  }
        key = mapKey(key)
				if (!(skipHeader && x._1.get()==0)){
					(key, value)
				}else{
				  null
				}
      });
    }
    return processedInput;
  }
  
  //KV to KV
	def sparkProcessKeyValue(input:RDD[Tuple2[String,String]], 
	    inputFormatClass:Class[_ <: InputFormat[LongWritable,Text]], spark:SparkSession): RDD[Tuple2[String,String]]= {
		return null
	}
}