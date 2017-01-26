package bdaps.engine.cmd

class CsvTransformCmd(wfName:String, wfid:String, staticCfg:String, prefix:String, 
      defaultFs:String) extends ACmd(wfName, wfid, staticCfg, prefix, defaultFs){
  
  override def init {
    super.init
  }
}