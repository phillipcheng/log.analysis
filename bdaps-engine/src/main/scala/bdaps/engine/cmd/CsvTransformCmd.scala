package bdaps.engine.cmd

import bdaps.engine.core.SchemaCmd;

class CsvTransformCmd(wfName:String, wfid:String, staticCfg:String, prefix:String, 
      defaultFs:String) extends SchemaCmd(wfName, wfid, staticCfg, prefix, defaultFs){
  
  override def init {
    super.init
  }
}