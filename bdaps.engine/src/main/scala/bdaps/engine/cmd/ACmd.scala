package bdaps.engine.cmd

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import etl.engine.EngineUtil


class ACmd(val wfName:String, val wfid:String, val staticCfg:String, val prefix:String, 
      val defaultFs:String) extends Serializable{
  @transient
  private var pc : PropertiesConfiguration = null
  
  def init {
    if (pc==null){
      pc = EngineUtil.getInstance.getMergedPC(staticCfg)
    }
  }
  
  def sparkProcessFilesToKV(inputfiles:RDD[String],  
			inputFormatClass:Class[_ <: InputFormat[_,_]], spark:SparkSession) : RDD[Tuple2[String,String]] = {
    inputfiles.flatMap[Tuple2[String,String]](s=>List((s,s)))
    return null;
  }
}