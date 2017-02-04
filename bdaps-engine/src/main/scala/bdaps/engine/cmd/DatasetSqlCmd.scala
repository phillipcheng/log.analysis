package bdaps.engine.cmd


import etl.cmd.transform.TableIdx;
import etl.util._;

import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import bdaps.engine.core.SchemaCmd;
import bdaps.engine.core.ACmdConst;

object DatasetSqlCmdConst{
	val no_table_value="no";
	val key_idx="idx";

	val cfgkey_sqls = "sqls";
	val cfgkey_new_tables = "new.tables";
}

class DatasetSqlCmd(wfName:String, wfid:String, staticCfg:String, prefix:String, 
      defaultFs:String) extends SchemaCmd(wfName, wfid, staticCfg, prefix, defaultFs){
  var sqlsMap : Seq[(String,String)] = null;//newTable name to sql mapping
	var idxMap : java.util.Map[String, TableIdx] = null;
	
	init
	
  override def init {
    super.init
    val sqls = pc.getStringArray(DatasetSqlCmdConst.cfgkey_sqls);
		var newtables = pc.getStringArray(DatasetSqlCmdConst.cfgkey_new_tables);
		if (newtables.length==0) newtables = Array(ACmdConst.SINGLE_TABLE)
		sqlsMap = sqls.zip(newtables)
		idxMap = new java.util.HashMap[String, TableIdx]();
		val it = pc.getKeys(DatasetSqlCmdConst.key_idx);
		while (it.hasNext()){
			val key = it.next().asInstanceOf[String];
			val name = key.substring(key.indexOf(DatasetSqlCmdConst.key_idx)+DatasetSqlCmdConst.key_idx.length()+1);
			val nidxExp = pc.getString(key);
			val dotIdx = nidxExp.indexOf(".");
			var tn = ACmdConst.SINGLE_TABLE;
			if (dotIdx != -1){
				tn = nidxExp.substring(0, dotIdx);
			}
			val idxExp = nidxExp.substring(dotIdx+1); 
			idxMap.put(name, new TableIdx(tn, IdxRange.parseString(idxExp)));
		}
		logger.info(String.format("idxMap:%s", idxMap));
  }
  
	override def useSparkSql():Boolean = {
		return true;
	}
	
	override def dataSetProcess(spark:SparkSession, singleTableColNum:Int):RDD[(String,String)] = {
	  val keyIt = idxMap.keySet().iterator();
	  while (keyIt.hasNext()){
	    val vn = keyIt.next();
			val ti:TableIdx = idxMap.get(vn);
			if (ACmdConst.SINGLE_TABLE == ti.getTableName()){
				ti.setColNum(singleTableColNum);
			}else{
				ti.setColNum(getLogicSchema().getAttrNames(ti.getTableName()).size());
			}
		}
	  val ret = sqlsMap.map(rt=>{
			val cookedSql=DBUtil.updateVar(rt._1, idxMap, this.getLogicSchema());
			val r = spark.sql(cookedSql).rdd;//execute sql
			if (!(DatasetSqlCmdConst.no_table_value == rt._2)){//output
			  r.map(t=>(rt._2,SchemaUtils.convertToString(t, csvValueSep)));
			}else{
			  spark.sparkContext.emptyRDD[(String,String)]
			}
		});
	  ret.reduce((left,right)=> if (left.isEmpty()) right else left ++ right)
	}
}