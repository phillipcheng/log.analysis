package etl.cmd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.CompiledScript;

import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

import etl.engine.EngineUtil;
import etl.util.ScriptEngineUtil;

public class KafkaMsgGenCmd extends SchemaFileETLCmd{
	private static final long serialVersionUID = 1L;
	public static final Logger logger = Logger.getLogger(KafkaMsgGenCmd.class);
	
	public static final String cfgkey_entity_name="entity.name";
	public static final String cfgkey_entity_attr_exp="entity.exp";

	private String entityName;
	
	private transient KafkaAdaptorCmd kac;
	private transient Map<String, CompiledScript> expMap;
	private transient Producer<String, String> producer;
	
	public KafkaMsgGenCmd(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfid, staticCfg, defaultFs, otherArgs);
	}
	
	public void init(String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		super.init(wfid, staticCfg, defaultFs, otherArgs);
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
		if (producer==null){
			producer = EngineUtil.createProducer(kac.getBootstrapServers());
		}
		EngineUtil.sendMsg(producer, kac.getLogTopicName(), sb.toString());
		return null;
	}
	
	@Override
	public void close(){
		logger.info("close producer");
		if (producer!=null){
			producer.close();
		}
	}

}
