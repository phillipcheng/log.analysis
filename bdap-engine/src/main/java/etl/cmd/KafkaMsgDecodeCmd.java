package etl.cmd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import etl.engine.EngineUtil;
import etl.util.FieldType;

public class KafkaMsgDecodeCmd extends SchemaFileETLCmd{
	private static final long serialVersionUID = 1L;
	public static final Logger logger = LogManager.getLogger(KafkaMsgDecodeCmd.class);
	//cfgkey
	public static final String cfgkey_entity_name="entity.name";
	
	private String entityName;
	private transient KafkaAdaptorCmd kac;
	private transient Consumer<String, String> consumer;
	
	public KafkaMsgDecodeCmd(){
		super();
	}
	
	public KafkaMsgDecodeCmd(String wfName, String wfid, String staticCfg, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	public KafkaMsgDecodeCmd(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		init(wfName, wfid, staticCfg, null, defaultFs, otherArgs);
	}
	
	@Override
	public void init(String wfName, String wfid, String staticCfg, String prefix, String defaultFs, String[] otherArgs){
		super.init(wfName, wfid, staticCfg, prefix, defaultFs, otherArgs);
		entityName = super.getCfgString(cfgkey_entity_name, null);
		if (entityName==null){
			logger.error(String.format("entityName can not be null."));
		}
		kac = new KafkaAdaptorCmd(super.getPc());
	}
	
	public Map<String, Object> decode(String v){
		List<FieldType> ftlist = logicSchema.getAttrTypes(entityName);
		List<String> attrList = logicSchema.getAttrNames(entityName);
		Map<String, Object> ret = new HashMap<String, Object>();
		String[] vs = v.split(",", -1);
		for (int i=0; i<ftlist.size(); i++){
			FieldType ft = ftlist.get(i);
			String sv = vs[i];
			ret.put(attrList.get(i), ft.decode(sv));
		}
		return ret;
	}
	
	@Override
	public List<String> sgProcess() {
		super.init();
		if (consumer==null){
			consumer = EngineUtil.createConsumer(kac.getBootstrapServers(), new String[]{kac.getLogTopicName()});
		}
		ConsumerRecords<String, String> rs = consumer.poll(100);
    	for (ConsumerRecord<String, String> r: rs){
    		try {
	            logger.info(String.format("offset = %d, key = %s, value = %s", r.offset(), r.key(), r.value()));
	            Map<String, Object> map = decode(r.value());
	            logger.info(String.format("map got:%s", map));
    		}catch(Exception e){
    			logger.error("", e);
    		}
    	}
    	logger.info(String.format("# records:%d", rs.count()));
        
		return null;
	}
	
	@Override
	public void close(){
		logger.info("close consumer");
		if (consumer!=null){
			consumer.close();
		}
	}
}
