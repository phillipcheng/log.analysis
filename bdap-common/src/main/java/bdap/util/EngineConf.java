package bdap.util;

import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class EngineConf {
	public static final Logger logger = LogManager.getLogger(EngineConf.class);
	
	public static final String file_name="etlengine.properties";
	
	public static final String cfgkey_defaultFs="defaultFs";
	
	public static final String cfgkey_kafka_bootstrap_servers="kafka.bootstrap.servers";
	public static final String cfgkey_kafka_log_topic="kafka.log.topic";
	public static final String cfgkey_kafka_enabled="kafka.enabled";
	
	public static final String cfgkey_db_type="db.type";
	public static final String cfgkey_db_driver="db.driver";
	public static final String cfgkey_db_url="db.url";
	public static final String cfgkey_db_user="db.user";
	public static final String cfgkey_db_password="db.password";
	
	public static final String cfgkey_hdfs_webhdfs_root="hdfs.webhdfs.root";
	
	//cfgkeys for InvokeMapper
	public static final String cfgkey_cmdclassname = "cmdClassName";
	public static final String cfgkey_wfid = "wfid";
	public static final String cfgkey_wfName = "wfName";
	public static final String cfgkey_staticconfigfile = "staticConfigFile";
	
	public static final String etlcmd_main_class="etl.engine.ETLCmdMain";
	public static final String mapper_class="etl.engine.InvokeMapper";
	public static final String reducer_class="etl.engine.InvokeReducer";
	
	
	PropertiesConfiguration conf;
	
	
	public EngineConf(){
	}
	
	public EngineConf(String file){
		InputStream input = this.getClass().getClassLoader().getResourceAsStream(file);
		if (input!=null){
			try {
				conf = new PropertiesConfiguration();
				conf.load(input);
			}catch(Exception e){
				logger.error("", e);
			}
		}else{
			logger.error(String.format("%s not found.", file));
		}
	}
	
	public PropertiesConfiguration getPropertiesConf(){
		return conf;
	}
	
	public void writeFile(String file){
		try {
			conf.save(file);
		} catch (ConfigurationException e) {
			logger.error("", e);
		}
	}
}
