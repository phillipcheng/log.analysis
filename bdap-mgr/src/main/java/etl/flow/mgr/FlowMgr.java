package etl.flow.mgr;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.fasterxml.jackson.module.jsonSchema.types.ArraySchema;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;
import com.fasterxml.jackson.module.jsonSchema.types.SimpleTypeSchema;
import com.fasterxml.jackson.module.jsonSchema.types.StringSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ValueTypeSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ArraySchema.Items;
import com.fasterxml.jackson.module.jsonSchema.types.BooleanSchema;
import com.fasterxml.jackson.module.jsonSchema.types.IntegerSchema;
import com.fasterxml.jackson.module.jsonSchema.types.NumberSchema;

import bdap.util.EngineConf;
import bdap.util.FileType;
import bdap.util.JsonUtil;
import bdap.util.PropertiesUtil;
import bdap.util.Util;
import etl.flow.ActionNode;
import etl.flow.CoordConf;
import etl.flow.Data;
import etl.flow.EndNode;
import etl.flow.ExeType;
import etl.flow.Flow;
import etl.flow.Node;
import etl.flow.StartNode;
import etl.flow.deploy.FlowDeployer;
import etl.flow.oozie.OozieConf;
import etl.util.ConfigKey;

public abstract class FlowMgr {
	public static final Logger logger = LogManager.getLogger(FlowMgr.class);
	private static final String CMD_PARAMETER_PREFIX = "cfgkey_";
	private static final JsonSchema[] EMPTY_SCHEMAS = new JsonSchema[0];
	
	public static String getPropFileName(String nodeName){
		return String.format("action_%s.properties", nodeName);
	}
	
	//generate the properties files for all the cmd to initiate
	public static List<InMemFile> genProperties(Flow flow){
		List<InMemFile> propertyFiles = new ArrayList<InMemFile>();
		for (Node n: flow.getNodes()){
			if (n instanceof ActionNode){
				ActionNode an = (ActionNode) n;
				String propFileName = getPropFileName(an.getName());
				byte[] bytes = PropertiesUtil.getPropertyFileContent(an.getAllProperties());
				propertyFiles.add(new InMemFile(FileType.actionProperty, propFileName, bytes));
			}
		}
		return propertyFiles;
	}
	
	public static InMemFile genEnginePropertyFile(EngineConf ec){
		return new InMemFile(FileType.engineProperty, EngineConf.file_name, ec.getContent());
	}
	
	//projectDir is hdfsDir
	public static String getDir(FileType ft, String projectDir, String flowName, OozieConf oc){
		String nameNodePath = oc.getNameNode();
		if (nameNodePath.endsWith("/")){
			nameNodePath = nameNodePath.substring(0, nameNodePath.length() - 1);
		}
		String hdfsDir = projectDir;
		if (hdfsDir.endsWith("/")){
			hdfsDir = hdfsDir.substring(0, hdfsDir.length() - 1);
		}
		if (hdfsDir.startsWith("/")){
			hdfsDir = hdfsDir.substring(1);
		}
		if (ft == FileType.actionProperty || ft == FileType.engineProperty || 
				ft==FileType.thirdpartyJar || ft == FileType.ftmappingFile || 
				ft==FileType.log4j || ft == FileType.logicSchema || ft == FileType.shell){
			return String.format("%s/%s/%s/lib/", nameNodePath, hdfsDir, flowName);
		}else if (ft == FileType.oozieWfXml || ft == FileType.oozieCoordXml){
			return String.format("%s/%s/%s/", nameNodePath, hdfsDir, flowName);
		}else{
			logger.error("file type not supported:%s", ft);
			return null;
		}
	}
	
	public static void uploadFiles(String projectDir, String flowName, List<InMemFile> files, FlowDeployer fd) {
		//deploy to the server
		for (InMemFile im:files){
			String dir = getDir(im.getFileType(), projectDir, flowName, fd.getOozieServerConf());
			String path = String.format("%s%s", dir, im.getFileName());
			logger.info(String.format("copy to %s", path));
			fd.deploy(path, im.getContent());
		}
	}
	
	//return common properties: nameNode, jobTracker, queue, username, useSystem
	public static bdap.xml.config.Configuration getCommonConf(OozieConf oc, String prjName, String wfName){
		bdap.xml.config.Configuration bodyConf = new bdap.xml.config.Configuration();
		{
			bdap.xml.config.Configuration.Property propNameNode = new bdap.xml.config.Configuration.Property();
			propNameNode.setName(OozieConf.key_nameNode);
			propNameNode.setValue(oc.getNameNode());
			bodyConf.getProperty().add(propNameNode);
		}{
			bdap.xml.config.Configuration.Property propJobTracker = new bdap.xml.config.Configuration.Property();
			propJobTracker.setName(OozieConf.key_jobTracker);
			propJobTracker.setValue(oc.getJobTracker());
			bodyConf.getProperty().add(propJobTracker);
		}{
			bdap.xml.config.Configuration.Property queueName = new bdap.xml.config.Configuration.Property();
			queueName.setName(OozieConf.key_queueName);
			queueName.setValue(oc.getQueueName());
			bodyConf.getProperty().add(queueName);
		}{
			bdap.xml.config.Configuration.Property propWfAppPath = new bdap.xml.config.Configuration.Property();
			propWfAppPath.setName(OozieConf.key_user_name);
			propWfAppPath.setValue(oc.getUserName());
			bodyConf.getProperty().add(propWfAppPath);
		}{
			bdap.xml.config.Configuration.Property oozieLibPath = new bdap.xml.config.Configuration.Property();
			oozieLibPath.setName(OozieConf.key_useSystemPath);
			oozieLibPath.setValue("true");
			bodyConf.getProperty().add(oozieLibPath);
		}{
			bdap.xml.config.Configuration.Property flowName = new bdap.xml.config.Configuration.Property();
			flowName.setName(OozieConf.key_flowName);
			flowName.setValue(wfName);
			bodyConf.getProperty().add(flowName);
		}{
			bdap.xml.config.Configuration.Property prjNameProp = new bdap.xml.config.Configuration.Property();
			prjNameProp.setName(OozieConf.key_prjName);
			prjNameProp.setValue(prjName);
			bodyConf.getProperty().add(prjNameProp);
		} if (oc.getOozieWfNtfUrl() != null && oc.getOozieWfNtfUrl().length() > 0) {
			bdap.xml.config.Configuration.Property oozieWfNtfUrlProp = new bdap.xml.config.Configuration.Property();
			oozieWfNtfUrlProp.setName(OozieConf.key_oozieWfNtfUrl);
			oozieWfNtfUrlProp.setValue(oc.getOozieWfNtfUrl());
			bodyConf.getProperty().add(oozieWfNtfUrlProp);
		} if (oc.getOozieWfActionNtfUrl() != null && oc.getOozieWfActionNtfUrl().length() > 0) {
			bdap.xml.config.Configuration.Property oozieWfActionNtfUrlProp = new bdap.xml.config.Configuration.Property();
			oozieWfActionNtfUrlProp.setName(OozieConf.key_oozieWfActionNtfUrl);
			oozieWfActionNtfUrlProp.setValue(oc.getOozieWfActionNtfUrl());
			bodyConf.getProperty().add(oozieWfActionNtfUrlProp);
		}
		return bodyConf;
	}
	
	public abstract boolean deployFlow(String prjName, Flow flow, String[] jars, FlowDeployer fd) throws Exception;
	public abstract String executeFlow(String prjName, String flowName, FlowDeployer fd) throws Exception;
	public abstract String executeCoordinator(String prjName, String flowName, FlowDeployer fd, CoordConf cc)  throws Exception;
	/**
	 * execute an action node of an existing workflow instance
	 * @param projectName
	 * @param flow
	 * @param imFiles
	 * @param fsconf
	 * @param ec
	 * @param actionNode
	 * @param instanceId
	 * @return
	 */
	public abstract void executeAction(String projectName, Flow flow, List<InMemFile> imFiles, FlowServerConf fsconf, 
			EngineConf ec, String actionNode, String instanceId);
	
	/**
	 * get the flow info of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @return flow info
	 */
	
	public abstract FlowInfo getFlowInfo(String projectName, FlowServerConf fsconf, String instanceId);
	
	/**
	 * get the flow log of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return log content
	 */
	public abstract String getFlowLog(String projectName, FlowServerConf fsconf, String instanceId);
	public abstract Resource getFlowLogResource(String projectName, FlowServerConf fsconf, String instanceId);
	
	/**
	 * get the action node log of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return log content
	 */
	public abstract InMemFile[] getNodeLog(String projectName, FlowServerConf fsconf, String instanceId, String nodeName);
	public abstract Resource[] getNodeLogResources(String projectName, FlowServerConf fsconf, String instanceId, String nodeName);
	
	/**
	 * get the action node info of submitted instance
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return node info
	 */
	public abstract NodeInfo getNodeInfo(String projectName, FlowServerConf fsconf, String instanceId, String nodeName);
	
	/**
	 * list the action node's input files
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return list of file paths
	 */
	public abstract String[] listNodeInputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId, String nodeName);
	
	/**
	 * list the action node's output files
	 * @param projectName
	 * @param fsconf
	 * @param instanceId
	 * @param nodeName
	 * @return list of file paths
	 */
	public abstract String[] listNodeOutputFiles(String projectName, FlowServerConf fsconf, EngineConf ec, String instanceId, String nodeName);


	/**
	 * get the distributed file according to the data definition
	 * @param engine config
	 * @param data definition
	 * @return file content
	 */
	public abstract InMemFile getDFSFile(EngineConf ec, Data data);

	/**
	 * get the distributed file according to the data definition & flow instance info
	 * @param engine config
	 * @param data definition
	 * @param flow instance info
	 * @return file content
	 */
	public abstract InMemFile getDFSFile(EngineConf ec, Data data, FlowInfo flowInfo);
	
	/**
	 * get the distributed file
	 * @param engine config
	 * @param filePath
	 * @return file content (Max file default size: 1MB)
	 */
	public abstract InMemFile getDFSFile(EngineConf ec, String filePath);
	

	/**
	 * get the distributed file
	 * @param engine config
	 * @param filePath
	 * @param maxFileSize
	 * @return file content
	 */
	public abstract InMemFile getDFSFile(EngineConf ec, String filePath, int maxFileSize);

	/**
	 * get the distributed file
	 * @param engineConfig
	 * @param filePath
	 * @param startLine
	 * @param endLine
	 * @return
	 */
	public abstract InMemFile getDFSFile(EngineConf ec, String filePath, long startLine, long endLine);

	/**
	 * put the distributed file
	 * @param engine config
	 * @param filePath
	 * @param file
	 * @return true/false
	 */
	public abstract boolean putDFSFile(EngineConf ec, String filePath, InMemFile file);
	
	//generate json file from java construction
	public static void genFlowJson(String rootFolder, String projectName, Flow flow){
		String jsonFileName=String.format("%s/%s/%s/%s.json", rootFolder, projectName, flow.getName(), flow.getName());
		String jsonString = JsonUtil.toJsonString(flow);
		Util.writeFile(jsonFileName, jsonString);
	}
	
	public static JsonSchema getFlowSchema(List<Class<?>> etlCmdClasses) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
		JsonSchema schema = schemaGen.generateSchema(Flow.class);
		JsonSchema nodeSchema = schemaGen.generateSchema(Node.class);
		JsonSchema actionNodeSchema = schemaGen.generateSchema(ActionNode.class);
		JsonSchema startNodeSchema = schemaGen.generateSchema(StartNode.class);
		JsonSchema endNodeSchema = schemaGen.generateSchema(EndNode.class);
		
		JsonSchema actionNodeSchemaBase = new ObjectSchema();
		actionNodeSchemaBase.setId(actionNodeSchema.getId());
		
		List<JsonSchema> nodeSchemas;
		
		if (schema instanceof ObjectSchema) {
			JsonSchema s;
			if (schema.asObjectSchema() != null && schema.asObjectSchema().getProperties() != null) {
				s = schema.asObjectSchema().getProperties().get("nodes");
				if (s instanceof ArraySchema) {
					if (s.asArraySchema() != null && s.asArraySchema().getItems() != null) {
						Items items = s.asArraySchema().getItems();
						if (items instanceof ArraySchema.SingleItems && items.asSingleItems() != null && items.asSingleItems().getSchema() != null) {
							StringSchema cmdClassSchema;
							StringSchema exeTypeSchema;
							JsonSchema valueSchema;
							nodeSchemas = new ArrayList<JsonSchema>();
							
							if (nodeSchema instanceof ObjectSchema && nodeSchema.asObjectSchema() != null)
								nodeSchema.asObjectSchema().setProperties(Collections.emptyMap());
							
							nodeSchemas.add(items.asSingleItems().getSchema());
							
							if (actionNodeSchema instanceof ObjectSchema && actionNodeSchema.asObjectSchema() != null) {
								cmdClassSchema = new StringSchema();
								if (etlCmdClasses != null)
									cmdClassSchema.setEnums(classNameSet(etlCmdClasses));
								actionNodeSchema.asObjectSchema().putProperty("cmd.class", cmdClassSchema);
								exeTypeSchema = new StringSchema();
								exeTypeSchema.setEnums(new HashSet<String>());
								ExeType[] exeTypeValues = ExeType.values();
								for (ExeType v: exeTypeValues)
									exeTypeSchema.getEnums().add(v.name());
								actionNodeSchema.asObjectSchema().putProperty("exe.type", exeTypeSchema);
								actionNodeSchema.asObjectSchema().setExtends(new JsonSchema[] {nodeSchema});
							}
							
							nodeSchemas.add(actionNodeSchema);
							
							if (startNodeSchema instanceof ObjectSchema && startNodeSchema.asObjectSchema() != null)
								startNodeSchema.asObjectSchema().setExtends(new JsonSchema[] {nodeSchema});
							
							nodeSchemas.add(startNodeSchema);
							
							if (endNodeSchema instanceof ObjectSchema && endNodeSchema.asObjectSchema() != null)
								endNodeSchema.asObjectSchema().setExtends(new JsonSchema[] {nodeSchema});
							
							nodeSchemas.add(endNodeSchema);
							
							/* List of action nodes with different Cmd Class */
							if (etlCmdClasses != null) {
								ObjectSchema cmdSchema;
								String name;
								Field[] fields;
								Object obj;
								for (Class<?> cls: etlCmdClasses) {
									name = cls.getCanonicalName();
									if (name != null) {
										cmdSchema = new ObjectSchema();
										cmdClassSchema = new StringSchema();
										cmdClassSchema.setDefault(name);
										cmdClassSchema.setReadonly(true);
										cmdSchema.putProperty("cmd.class", cmdClassSchema);
										
										/* Cmd class properties */
										fields = cls.getFields();
										if (fields != null) {
											for (Field f: fields) {
											    if (Modifier.isStatic(f.getModifiers()) && (f.getName().startsWith(CMD_PARAMETER_PREFIX) ||
											    		f.isAnnotationPresent(ConfigKey.class))) {
													ConfigKey ck = f.getAnnotation(ConfigKey.class);
													logger.trace(ck);
													logger.trace(f.getName());
											        try {
														obj = FieldUtils.readStaticField(f, true);
												        if (obj != null) {
												        	if (ck != null && Boolean.class.equals(ck.type()))
												        		valueSchema = new BooleanSchema();
												        	else if (ck != null && Integer.class.equals(ck.type()))
												        		valueSchema = new IntegerSchema();
												        	else if (ck != null && Long.class.equals(ck.type()))
												        		valueSchema = new NumberSchema();
												        	else if (ck != null && Double.class.equals(ck.type()))
												        		valueSchema = new NumberSchema();
												        	else if (ck != null && Float.class.equals(ck.type()))
												        		valueSchema = new NumberSchema();
												        	else if (ck != null && ck.type().isArray()) {
												        		valueSchema = new ArraySchema();
												        		if (String.class.equals(ck.type().getComponentType()))
												        			valueSchema.asArraySchema().setItemsSchema(new StringSchema());
												        	} else if (ck != null && ck.type().isEnum()) {
												        		valueSchema = new StringSchema();
												        		valueSchema.asStringSchema().setEnums(enumSet(ck.type()));
												        	} else
												        		valueSchema = new StringSchema();
												        	if (ck != null && ck.defaultValue() != null && valueSchema instanceof SimpleTypeSchema)
												        		valueSchema.asSimpleTypeSchema().setDefault(ck.defaultValue());
												        	if (ck != null && ck.format() != null && ck.format().length() > 0 && valueSchema instanceof ValueTypeSchema) {
												        		if (ck.format().startsWith("enum:")) {
												        			valueSchema.asValueTypeSchema().setEnums(enumFormatSet(ck.format()));
												        		}
												        			
												        	}
															cmdSchema.putOptionalProperty(obj.toString(), valueSchema);
												        }
													} catch (IllegalAccessException e) {
														logger.error(e.getMessage(), e);
													}
											        
											    }
											}
										}
										cmdSchema.setExtends(new JsonSchema[] {actionNodeSchemaBase});
										nodeSchemas.add(cmdSchema);
									}
								}
							}
							
							ArraySchema.ArrayItems newItems = new ArraySchema.ArrayItems(nodeSchemas.toArray(EMPTY_SCHEMAS));
							s.asArraySchema().setItems(newItems);
						}
					}
				}
			}
		}
	    return schema;
	}

	private static Set<String> enumFormatSet(String format) {
		Set<String> names = new HashSet<String>();
		if (format != null && format.length() > 0) {
			format = format.substring(5);
			String[] vals = format.split("\\|");
			if (vals != null)
				for (String v: vals)
					names.add(v);
		}
		return names;
	}

	private static Set<String> enumSet(Class<?> type) {
		Object[] objs = type.getEnumConstants();
		Set<String> names = new HashSet<String>();
		if (objs != null)
			for (Object o: objs)
				if (o != null)
					names.add(o.toString());
		return names;
	}

	private static Set<String> classNameSet(List<Class<?>> etlCmdClasses) {
		Set<String> classNames = new HashSet<String>();
		if (etlCmdClasses != null) {
			String name;
			for (Class<?> cls: etlCmdClasses) {
				name = cls.getCanonicalName();
				if (name != null)
					classNames.add(name);
			}
		}
		return classNames;
	}
}
