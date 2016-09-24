package etl.cmd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import etl.engine.EngineUtil;
import etl.util.ScriptEngineUtil;

public class KafkaMsgGenCmd extends SchemaFileETLCmd{
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(KafkaMsgGenCmd.class);
	
	public static final String cfgkey_entity_name="entity.name";
	public static final String cfgkey_entity_attr_exp="entity.exp";

	private String entityName;
	
	private transient KafkaAdaptorCmd kac;
	private transient Map<String, CompiledScript> expMap;
	
	public KafkaMsgGenCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, defaultFs, otherArgs);
	}
	
	public void init(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, defaultFs, otherArgs);
		kac = new KafkaAdaptorCmd(pc);
		entityName = pc.getString(cfgkey_entity_name);
		expMap = new HashMap<String, CompiledScript>();
		List<String> attrs = logicSchema.getAttrNameMap().get(entityName);
		for (String attrName:attrs){
			String attrExp = pc.getString(cfgkey_entity_attr_exp+"."+attrName, null);
			if (attrExp!=null){
				expMap.put(attrName, ScriptEngineUtil.compileScript(attrExp));
			}
		}
	}
	
	@Override
	public List<String> sgProcess() {
		super.init();
		logger.info(String.format("use engine config: %b", EngineUtil.getInstance().isSendLog()));
		List<String> attrs = logicSchema.getAttrNameMap().get(entityName);
		StringBuffer sb = new StringBuffer();
		for (String attrName:attrs){
			CompiledScript cs = expMap.get(attrName);
			String v = "";
			if (cs!=null){
				v = ScriptEngineUtil.eval(cs, super.getSystemVariables());
			}
			sb.append(v).append(",");
		}
		
		EngineUtil.getInstance().sendMsg(kac.getLogTopicName(), null, sb.toString());
		return null;
	}
	
	@Override
	public void close(){
		logger.info("close producer");
	}

}
