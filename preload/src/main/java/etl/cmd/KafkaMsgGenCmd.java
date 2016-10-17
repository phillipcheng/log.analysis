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
	//cfgkey
	public static final String cfgkey_entity_name="entity.name";
	public static final String cfgkey_entity_attr_exp="entity.exp";
	public static final String cfgkey_entity_key="entity.key";
	
	private String entityName;
	
	private transient KafkaAdaptorCmd kac;
	private transient Map<String, CompiledScript> expMap;
	private transient CompiledScript keyCS;
	
	public KafkaMsgGenCmd(){
		super();
	}
	
	public KafkaMsgGenCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public KafkaMsgGenCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		kac = new KafkaAdaptorCmd(super.getPc());
		entityName = super.getCfgString(cfgkey_entity_name, null);
		if (entityName==null){
			logger.error(String.format("entityName can't be null."));
			return;
		}
		expMap = new HashMap<String, CompiledScript>();
		List<String> attrs = logicSchema.getAttrNameMap().get(entityName);
		for (String attrName:attrs){
			String attrExp = super.getCfgString(cfgkey_entity_attr_exp+"."+attrName, null);
			if (attrExp!=null){
				expMap.put(attrName, ScriptEngineUtil.compileScript(attrExp));
			}
		}
		String strEntityKey = super.getCfgString(cfgkey_entity_key, null);
		logger.info(String.format("entity key is %s.", strEntityKey));
		if (strEntityKey!=null){
			keyCS = ScriptEngineUtil.compileScript(strEntityKey);
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
		String key = null;
		if (keyCS!=null){
			key = ScriptEngineUtil.eval(keyCS, super.getSystemVariables());
		}
		logger.info(String.format("kafka-msg-gen-cmd:%s,%s", key, sb.toString()));
		EngineUtil.getInstance().sendMsg(kac.getLogTopicName(), key, sb.toString());
		return null;
	}
	
	@Override
	public void close(){
		logger.info("close producer");
	}

}
