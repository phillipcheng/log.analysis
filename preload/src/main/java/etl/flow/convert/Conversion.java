package etl.flow.convert;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import etl.flow.ActionNode;
import etl.flow.Flow;
import etl.flow.Node;
import etl.util.PropertiesUtil;
import etl.util.Util;

public class Conversion {
	public static final Logger logger = LogManager.getLogger(Conversion.class);
	//generate the properties files for all the cmd to initiate
	public static void genProperties(Flow flow, String dir){
		for (Node n: flow.getNodes()){
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileString = String.format("%s%s%s_%s.properties", dir, File.separator, flow.getName(), an.getName());
				PropertiesUtil.writePropertyFile(propFileString, an.getUserProperties());
			}
		}
	}
}
