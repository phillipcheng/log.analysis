package dv.mgr;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.fasterxml.jackson.module.jsonSchema.JsonSchema;

import bdap.util.EngineConf;
import bdap.util.FileType;
import bdap.util.JsonUtil;
import bdap.util.PropertiesUtil;
import dv.UserNotFoundException;
import dv.db.dao.AccountRepository;
import dv.db.dao.CommonDao;
import dv.db.dao.FlowInstanceRepository;
import dv.db.dao.FlowRepository;
import dv.db.dao.ProjectRepository;
import dv.db.entity.AccountEntity;
import dv.db.entity.FlowEntity;
import dv.db.entity.FlowInstanceEntity;
import dv.db.entity.ProjectEntity;
import dv.service.AppProjectClassLoader;
import dv.ws.WebSocketManager;
import etl.engine.ETLCmd;
import etl.flow.CallSubFlowNode;
import etl.flow.Data;
import etl.flow.Flow;
import etl.flow.Node;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;
import etl.flow.oozie.OozieConf;
import etl.util.ConfigKey;

@RestController
@RequestMapping("/{userName}/flow")
public class FlowController {
	public static final Logger logger = LogManager.getLogger(FlowController.class);
	private static final String config = "config/config.properties";
	private static final String[] SEARCH_PACKAGES = new String[] {"etl.cmd"};
	private static final String ACTION_NODE_CMDS_SEARCH_PACKAGES = "action.node.cmds.search.packages";
	private static final String CMD_PARAMETER_PREFIX = "cfgkey_";
	private static final String[] EMPTY_STRING_ARRAY = new String[0];
	private String[] searchPackages;
	
	@Autowired
	FlowController(FlowRepository flowRepository, AccountRepository accountRepository, ProjectRepository projectRepository, FlowMgr flowMgr, FlowDeployer flowDeployer,
			FlowInstanceRepository flowInstance, CommonDao commonDao) {
		this.flowRepository = flowRepository;
		this.accountRepository = accountRepository;
		this.projectRepository = projectRepository;
		this.flowMgr = flowMgr;
		this.flowDeployer = flowDeployer;
		this.flowInstanceRepository = flowInstance;
		this.commonDao = commonDao;
		
		PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(config);
		this.searchPackages = pc.getStringArray(ACTION_NODE_CMDS_SEARCH_PACKAGES);
		if (searchPackages == null || searchPackages.length == 0)
			searchPackages = SEARCH_PACKAGES;
	}

	private final ProjectRepository projectRepository;
	private final FlowRepository flowRepository;
	private final AccountRepository accountRepository;
	private final FlowMgr flowMgr;
	private final FlowDeployer flowDeployer;
	private final FlowInstanceRepository flowInstanceRepository;
	private final CommonDao commonDao;
	
	private List<Class<?>> getETLCmdClasses() {
		ClassPathScanningCandidateComponentProvider actionsProvider = new ClassPathScanningCandidateComponentProvider(false);
		actionsProvider.addIncludeFilter(new AssignableTypeFilter(ETLCmd.class));
		List<Class<?>> classes = new ArrayList<Class<?>>();
		Set<BeanDefinition> beanDefs;
		Class<?> cls;
		for (String pkgPath: searchPackages) {
			beanDefs = actionsProvider.findCandidateComponents(pkgPath);
			for (BeanDefinition def: beanDefs) {
				logger.debug(def.getBeanClassName());
				try {
					cls = Class.forName(def.getBeanClassName());
					if (cls != null)
						classes.add(cls);
				} catch (ClassNotFoundException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return classes;
	}
	
	@RequestMapping(value = "/schema", method = RequestMethod.GET)
	JsonSchema getFlowSchema() throws Exception {
	    return FlowMgr.getFlowSchema(getETLCmdClasses());
	}
	
	@RequestMapping(value = "/schema", method = RequestMethod.GET, params="projectId")
	JsonSchema getFlowSchema(@RequestParam(value = "projectId") int projectId) throws Exception {
		ProjectEntity pe = projectRepository.getOne(projectId);
		List<Class<?>> classes = getETLCmdClasses();
		if (pe != null) {
			String hdfsDir = JsonUtil.fromJsonString(pe.getContent(), "hdfsDir", String.class);
			String t = JsonUtil.fromJsonString(pe.getContent(), "cmdClassSearchPackages", String.class);
			List<String> searchPkgList = new ArrayList<String>();
			for (String s: searchPackages)
				searchPkgList.add(s);
			if (t != null) {
				String[] cmdClsSearchPkgs = t.split(",");
				for (String s: cmdClsSearchPkgs)
					searchPkgList.add(s);
			}
			AppProjectClassLoader clsLdr = new AppProjectClassLoader(flowDeployer.getDefaultFS(),
				flowDeployer.getDeployMethod(), hdfsDir);
			classes.addAll(clsLdr.findClasses(searchPkgList.toArray(EMPTY_STRING_ARRAY), ETLCmd.class));
		}
	    return FlowMgr.getFlowSchema(classes);
	}
	
	@RequestMapping(value = "/schema", method = RequestMethod.GET, params="projectName")
	JsonSchema getFlowSchema(@RequestParam(value = "projectName") String projectName) throws Exception {
		ProjectEntity pe = projectRepository.findByName(projectName);
		List<Class<?>> classes = getETLCmdClasses();
		if (pe != null) {
			String hdfsDir = JsonUtil.fromJsonString(pe.getContent(), "hdfsDir", String.class);
			String t = JsonUtil.fromJsonString(pe.getContent(), "cmdClassSearchPackages", String.class);
			List<String> searchPkgList = new ArrayList<String>();
			for (String s: searchPackages)
				searchPkgList.add(s);
			if (t != null) {
				String[] cmdClsSearchPkgs = t.split(",");
				for (String s: cmdClsSearchPkgs)
					searchPkgList.add(s);
			}
			AppProjectClassLoader clsLdr = new AppProjectClassLoader(flowDeployer.getDefaultFS(),
				flowDeployer.getDeployMethod(), hdfsDir);
			classes.addAll(clsLdr.findClasses(searchPkgList.toArray(EMPTY_STRING_ARRAY), ETLCmd.class));
		}
	    return FlowMgr.getFlowSchema(classes);
	}

	@RequestMapping(value = "/node/types/action/commands", method = RequestMethod.GET)
	Map<String, List<String>> getActionNodeCommands() {
		Map<String, List<String>> classNames = new HashMap<String, List<String>>();
		List<String> cmdParameters;
		Field[] fields;
		Object obj;
		List<Class<?>> classes = getETLCmdClasses();
		if (classes != null) {
			for (Class<?> cls: classes) {
				if (cls.getCanonicalName() != null) {
					cmdParameters = new ArrayList<String>();
					fields = cls.getFields();
					if (fields != null) {
						for (Field f: fields) {
						    if (Modifier.isStatic(f.getModifiers()) && (f.getName().startsWith(CMD_PARAMETER_PREFIX) ||
						    		f.isAnnotationPresent(ConfigKey.class))) {
						        try {
									obj = FieldUtils.readStaticField(f, true);
							        if (obj != null)
							        	cmdParameters.add(obj.toString());
								} catch (IllegalAccessException e) {
									logger.error(e.getMessage(), e);
								}
						    }
						}
					}
					classNames.put(cls.getCanonicalName(), cmdParameters);
				}
			}
		}
		return classNames;
	}
	
	@RequestMapping(value = "/node/types/action/commands", method = RequestMethod.GET, params="projectId")
	Map<String, List<String>> getActionNodeCommands(@RequestParam(value = "projectId") int projectId) {
		ProjectEntity pe = projectRepository.getOne(projectId);
		Map<String, List<String>> classNames = new HashMap<String, List<String>>();
		List<String> cmdParameters;
		Field[] fields;
		Object obj;
		List<Class<?>> classes = getETLCmdClasses();
		if (pe != null) {
			String hdfsDir = JsonUtil.fromJsonString(pe.getContent(), "hdfsDir", String.class);
			String t = JsonUtil.fromJsonString(pe.getContent(), "cmdClassSearchPackages", String.class);
			List<String> searchPkgList = new ArrayList<String>();
			for (String s: searchPackages)
				searchPkgList.add(s);
			if (t != null) {
				String[] cmdClsSearchPkgs = t.split(",");
				for (String s: cmdClsSearchPkgs)
					searchPkgList.add(s);
			}
			AppProjectClassLoader clsLdr = new AppProjectClassLoader(flowDeployer.getDefaultFS(),
				flowDeployer.getDeployMethod(), hdfsDir);
			classes.addAll(clsLdr.findClasses(searchPkgList.toArray(EMPTY_STRING_ARRAY), ETLCmd.class));
		}
		if (classes != null) {
			for (Class<?> cls: classes) {
				if (cls.getCanonicalName() != null) {
					cmdParameters = new ArrayList<String>();
					fields = cls.getFields();
					if (fields != null) {
						for (Field f: fields) {
						    if (Modifier.isStatic(f.getModifiers()) && (f.getName().startsWith(CMD_PARAMETER_PREFIX) ||
						    		f.isAnnotationPresent(ConfigKey.class))) {
						        try {
									obj = FieldUtils.readStaticField(f, true);
							        if (obj != null)
							        	cmdParameters.add(obj.toString());
								} catch (IllegalAccessException e) {
									logger.error(e.getMessage(), e);
								}
						    }
						}
					}
					classNames.put(cls.getCanonicalName(), cmdParameters);
				}
			}
		}
		return classNames;
	}
	
	@RequestMapping(value = "/node/types/action/commands", method = RequestMethod.GET, params="projectName")
	Map<String, List<String>> getActionNodeCommands(@RequestParam(value = "projectName") String projectName) {
		ProjectEntity pe = projectRepository.findByName(projectName);
		Map<String, List<String>> classNames = new HashMap<String, List<String>>();
		List<String> cmdParameters;
		Field[] fields;
		Object obj;
		List<Class<?>> classes = getETLCmdClasses();
		if (pe != null) {
			String hdfsDir = JsonUtil.fromJsonString(pe.getContent(), "hdfsDir", String.class);
			String t = JsonUtil.fromJsonString(pe.getContent(), "cmdClassSearchPackages", String.class);
			List<String> searchPkgList = new ArrayList<String>();
			for (String s: searchPackages)
				searchPkgList.add(s);
			if (t != null) {
				String[] cmdClsSearchPkgs = t.split(",");
				for (String s: cmdClsSearchPkgs)
					searchPkgList.add(s);
			}
			AppProjectClassLoader clsLdr = new AppProjectClassLoader(flowDeployer.getDefaultFS(),
				flowDeployer.getDeployMethod(), hdfsDir);
			classes.addAll(clsLdr.findClasses(searchPkgList.toArray(EMPTY_STRING_ARRAY), ETLCmd.class));
		}
		if (classes != null) {
			for (Class<?> cls: classes) {
				if (cls.getCanonicalName() != null) {
					cmdParameters = new ArrayList<String>();
					fields = cls.getFields();
					if (fields != null) {
						for (Field f: fields) {
						    if (Modifier.isStatic(f.getModifiers()) && (f.getName().startsWith(CMD_PARAMETER_PREFIX) ||
						    		f.isAnnotationPresent(ConfigKey.class))) {
						        try {
									obj = FieldUtils.readStaticField(f, true);
							        if (obj != null)
							        	cmdParameters.add(obj.toString());
								} catch (IllegalAccessException e) {
									logger.error(e.getMessage(), e);
								}
						    }
						}
					}
					classNames.put(cls.getCanonicalName(), cmdParameters);
				}
			}
		}
		return classNames;
	}
	
	@Deprecated
	@RequestMapping(method = RequestMethod.POST)
	ResponseEntity<?> add(@PathVariable String userName, @RequestBody Flow input) {
		this.validateUser(userName);
		FlowEntity fe = new FlowEntity(input.getName(),
				userName, JsonUtil.toJsonString(input));
		ProjectEntity pe = projectRepository.findByName("project1");
		if (pe != null)
			fe.setProjectId(pe.getId());
		else
			fe.setProjectId(0);
		fe.setDeployed(false);
		fe = flowRepository.save(fe);

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.setLocation(ServletUriComponentsBuilder
				.fromCurrentRequest().path("/{id}")
				.buildAndExpand(fe.getName()).toUri());
		return new ResponseEntity<>(null, httpHeaders, HttpStatus.CREATED);
	}
	
	@RequestMapping(method = RequestMethod.POST, params="projectId")
	ResponseEntity<?> add(@PathVariable String userName, @RequestBody Flow input, @RequestParam(value = "projectId") int projectId) {
		this.validateUser(userName);
		FlowEntity fe = new FlowEntity(input.getName(),
				userName, JsonUtil.toJsonString(input));
		fe.setProjectId(projectId);
		fe.setDeployed(false);
		fe = flowRepository.save(fe);

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.setLocation(ServletUriComponentsBuilder
				.fromCurrentRequest().path("/{id}")
				.buildAndExpand(fe.getName()).toUri());
		return new ResponseEntity<>(null, httpHeaders, HttpStatus.CREATED);
	}
	
	@RequestMapping(method = RequestMethod.POST, params="projectName")
	ResponseEntity<?> add(@PathVariable String userName, @RequestBody Flow input, @RequestParam(value = "projectName") String projectName) {
		this.validateUser(userName);
		FlowEntity fe = new FlowEntity(input.getName(),
				userName, JsonUtil.toJsonString(input));
		ProjectEntity pe = projectRepository.findByName(projectName);
		if (pe != null)
			fe.setProjectId(pe.getId());
		fe.setDeployed(false);
		fe = flowRepository.save(fe);

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.setLocation(ServletUriComponentsBuilder
				.fromCurrentRequest().path("/{id}")
				.buildAndExpand(fe.getName()).toUri());
		return new ResponseEntity<>(null, httpHeaders, HttpStatus.CREATED);
	}
	
	@RequestMapping(value = "/{flowId}", method = RequestMethod.GET)
	FlowEntity getFlow(@PathVariable String userName, @PathVariable String flowId) {
		this.validateUser(userName);
		return this.flowRepository.findOne(flowId);
	}
	
	@RequestMapping(value = "/list", method = RequestMethod.GET)
	List<Map<String, String>> getAllFlow(@PathVariable String userName) {
		this.validateUser(userName);
		List<FlowEntity> flowEntities = this.flowRepository.findAll();
		List<Map<String, String>> flows = new ArrayList<Map<String, String>>();
		if (flowEntities != null) {
			Map<String, String> info;
			ProjectEntity p;
			for (FlowEntity f: flowEntities) {
				info = new HashMap<String, String>();
				p = projectRepository.getOne(f.getProjectId());
				info.put("name", f.getName());
				info.put("projectName", p != null ? p.getProjectName() : "");
				info.put("owner", f.getOwner());
				info.put("deployed", Boolean.toString(f.isDeployed()));
				flows.add(info);
			}
		}
		return flows;
	}
	
	@RequestMapping(value = "/list/page", method = RequestMethod.GET)
	Map<String, Object> getPageFlow(@PathVariable String userName, @RequestParam(value = "page") int page, @RequestParam(value = "size") int size) {
		this.validateUser(userName);
		page = page -1;
		if( page < 0){
			page = 0;
		}
		if( size < 0){
			size = 10;
		}
		Pageable pageable = new PageRequest(page, size, new Sort("name"));  
//		Pageable pageable = new PageRequest(page, size, null);  
		Page<FlowEntity> flowEntities = this.flowRepository.findAll(pageable);
		long total = flowEntities.getTotalElements();
		Map json = new HashMap();
		List<Map<String, String>> flows = new ArrayList<Map<String, String>>();
		if (flowEntities != null) {
			Map<String, String> info;
			ProjectEntity p;
			for (FlowEntity f: flowEntities) {
				info = new HashMap<String, String>();
				p = projectRepository.getOne(f.getProjectId());
				info.put("name", f.getName());
				info.put("projectName", p != null ? p.getProjectName() : "");
				info.put("owner", f.getOwner());
				info.put("deployed", Boolean.toString(f.isDeployed()));
				flows.add(info);
			}
		}
		json.put("total", total);
		json.put("rows", flows);
		return json;
	}
	
	@RequestMapping(value = "/{flowId}/instance/{instanceid}/add", method = RequestMethod.GET)
	void add(@PathVariable String userName, @PathVariable String flowId, @PathVariable String instanceid) {
		this.validateUser(userName);
		FlowInstanceEntity entity = new FlowInstanceEntity();
		entity.setFlowName(flowId);
		entity.setInstanceID(instanceid);
		//SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date datetime=new java.util.Date();
		java.sql.Timestamp time=new java.sql.Timestamp(datetime.getTime());
		entity.setUpdateTime(time);
		this.flowInstanceRepository.save(entity);
	}
	
	@RequestMapping(value = "/instance/list", method = RequestMethod.GET)
	List<Map<String, Object>> getAllInstance(@PathVariable String userName) {
		this.validateUser(userName);
		String sql = "SELECT instanceid, flowname, owner, updatetime FROM T_FLOWINSTANCE inner join t_flow on flowname = name where owner = '"+userName+"'";
		return this.commonDao.getNativeMapBySQL(sql);
	}
	
	@RequestMapping(value = "/instance/list/page", method = RequestMethod.GET)
	Map<String, Object> getPageInstance(@PathVariable String userName, @RequestParam(value = "page") int page, @RequestParam(value = "size") int size) {
		this.validateUser(userName);
		page = page -1;
		if( page < 0){
			page = 0;
		}
		if( size < 0){
			size = 10;
		}
		String sql = "SELECT instanceid, flowname, owner, updatetime FROM T_FLOWINSTANCE inner join t_flow on flowname = name where owner = '"+userName+"'";
		List list = this.commonDao.getNativeMapBySQL(sql);
		int total = list.size();
		
		sql = "select  * from ( SELECT instanceid, flowname, owner, updatetime FROM T_FLOWINSTANCE inner join t_flow on flowname = name where owner = '" + userName
				+ "'  order by updatetime desc )"
				+" limit " + page * size + "," + size ;
		List pageList = this.commonDao.getNativeMapBySQL(sql);
		Map json = new HashMap();
		json.put("total", total);
		json.put("rows", pageList);
		return json;
	}
	
	@RequestMapping(value = "/allFlow/allInstance", method = RequestMethod.GET)
	List<Map> getAllFlowWithInstance(@PathVariable String userName) {
		this.validateUser(userName);
		List<FlowEntity> list = this.flowRepository.findAll();
		List<Map> allList = new ArrayList<Map>();
		for(FlowEntity obj : list) {
			Map map = new HashMap();
			List<FlowInstanceEntity> instanceList = this.flowInstanceRepository.findByFlowName(obj.getName());
			map.put("name", obj.getName());
			map.put("owner", obj.getOwner());
			List<Map> instanceMapList = new ArrayList<Map>();
			for(FlowInstanceEntity instanceObj : instanceList){
				Map instanceMap = new HashMap();
				instanceMap.put("name", instanceObj.getInstanceID());
				instanceMap.put("owner", obj.getOwner());
				instanceMapList.add(instanceMap);
			}
			map.put("children", instanceMapList);
			allList.add(map);
		}
		return allList;
	}
	
	@RequestMapping(value = "/{flowId}/del", method = RequestMethod.GET)
	 void delFlow(@PathVariable String userName, @PathVariable String flowId) {
		this.validateUser(userName);
		this.flowRepository.delete(flowId);
	}
	
	@RequestMapping(value = "/instance/{instanceId}/del", method = RequestMethod.GET)
	 void delFlowInstance(@PathVariable String userName, @PathVariable String instanceId) {
		this.validateUser(userName);
		this.flowInstanceRepository.delete(instanceId);
	}

	private void deployFlow(ProjectEntity pe, FlowEntity flowEntity) throws Exception {
		if (!flowEntity.isDeployed()) {
			Flow flow = JsonUtil.fromJsonString(flowEntity.getJsonContent(), Flow.class);
			if (flow != null && flow.getNodes() != null) {
				ProjectEntity subflowPE;
				FlowEntity subflowEntity;
				for (Node n: flow.getNodes()) {
					if (n instanceof CallSubFlowNode) {
						subflowEntity = flowRepository.getOne(((CallSubFlowNode) n).getSubFlowName());
						if (subflowEntity != null && subflowEntity.getJsonContent() != null && subflowEntity.getJsonContent().length() > 0) {
							subflowPE = projectRepository.getOne(flowEntity.getProjectId());
							if (subflowPE != null)
								deployFlow(subflowPE, subflowEntity);
						}
					}
				}
			}
			
			ClassLoader originalCl = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(new AppProjectClassLoader(flowDeployer.getDefaultFS(),
					flowDeployer.getDeployMethod(), FlowMgr.getDir(FileType.thirdpartyJar, flowDeployer.getProjectHdfsDir(pe.getProjectName()), flowEntity.getName(), flowDeployer.getOozieServerConf())));
			flowMgr.deployFlow(pe.getProjectName(), flow, null, null, flowDeployer);
			Thread.currentThread().setContextClassLoader(originalCl);
			flowEntity.setDeployed(true);
			flowRepository.save(flowEntity);
		}
	}
	
	@RequestMapping(value = "/{flowId}", method = RequestMethod.POST)
	ResponseEntity<?> execute(@PathVariable String userName, @PathVariable String flowId) {
		HttpHeaders httpHeaders = new HttpHeaders();
		this.validateUser(userName);
		try {
			FlowEntity flowEntity = flowRepository.getOne(flowId);
			if (flowEntity != null && flowEntity.getJsonContent() != null && flowEntity.getJsonContent().length() > 0) {
				ProjectEntity pe = projectRepository.getOne(flowEntity.getProjectId());
				if (pe != null) {
					String flowInstanceId;
					deployFlow(pe, flowEntity);
					flowInstanceId = flowMgr.executeFlow(pe.getProjectName(), flowId, flowDeployer);
					return new ResponseEntity<String>(flowInstanceId, httpHeaders, HttpStatus.CREATED);
				} else {
					return new ResponseEntity<String>("Project entity not found or is empty", httpHeaders, HttpStatus.NOT_FOUND);
				}
			} else {
				return new ResponseEntity<String>("Flow entity not found or is empty", httpHeaders, HttpStatus.NOT_FOUND);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return new ResponseEntity<String>(e.getMessage(), httpHeaders, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@RequestMapping(value = "/{flowId}/data/{dataName:.+}", method = RequestMethod.GET)
	InMemFile getFlowData(@PathVariable String userName, @PathVariable String flowId, @PathVariable String dataName) {
		this.validateUser(userName);
		FlowEntity flowEntity = this.flowRepository.findOne(flowId);
		if (flowEntity != null && flowEntity.getJsonContent() != null && flowEntity.getJsonContent().length() > 0) {
			Flow flow = JsonUtil.fromJsonString(flowEntity.getJsonContent(), Flow.class);
			List<Data> datalist = flow.getData();
			if (datalist != null) {
				EngineConf ec = flowDeployer.getEngineConfig();
				for (Data d: datalist) {
					if (d != null && d.getName() != null && !d.isInstance() && d.getName().equals(dataName)) {
						return this.flowMgr.getDFSFile(ec, d);
					}
				}
			}
			return null;
		} else {
			logger.debug("Flow {} not found", flowId);
			return null;
		}
	}
	
	@RequestMapping(value = "/instances/{instanceId}/status", method = RequestMethod.GET)
	String notifyFlowInstanceStatus(@PathVariable String userName, @PathVariable String instanceId, @RequestParam(value = "status") String status) throws IOException {
		String message = "{\"instanceId\": \"" + instanceId + "\", " + "\"status\": \"" + status + "\"}";
		logger.debug(message);
		WebSocketManager.sendMassMessage(instanceId, message);
		return "";
	}
	
	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/status", method = RequestMethod.GET)
	String notifyFlowNodeStatus(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName, @RequestParam(value = "status") String status) throws IOException {
		String message;
		if (status != null && status.startsWith("T:")) {
			String targetNode = status.substring(2);
			if ("*".equals(targetNode)) {
				status = "OK";
				
			} else if ("null".equals(targetNode)) {
				status = "OK";
				
			} else if ("fail".equals(targetNode)) {
				status = "ERROR";
				message = "{\"instanceId\": \"" + instanceId + "\", " + "\"nodeName\": \"" + nodeName + "\", " + "\"status\": \"" + status + "\"}";
				logger.debug(message);
				WebSocketManager.sendMassMessage(instanceId, message);

				nodeName = targetNode;
				status = "PREP";
				
			} else {
				status = "OK";
				message = "{\"instanceId\": \"" + instanceId + "\", " + "\"nodeName\": \"" + nodeName + "\", " + "\"status\": \"" + status + "\"}";
				logger.debug(message);
				WebSocketManager.sendMassMessage(instanceId, message);
				
				nodeName = targetNode;
				status = "PREP";
			}
		} else if (status != null && status.startsWith("S:"))
			status = status.substring(2);
		
		message = "{\"instanceId\": \"" + instanceId + "\", " + "\"nodeName\": \"" + nodeName + "\", " + "\"status\": \"" + status + "\"}";
		logger.debug(message);
		WebSocketManager.sendMassMessage(instanceId, message);
		return "";
	}
	
	@RequestMapping(value = "/instances/{instanceId}", method = RequestMethod.GET)
	FlowInfo getFlowInstance(@PathVariable String userName, @PathVariable String instanceId) {
		OozieConf oc = flowDeployer.getOozieServerConf();
		this.validateUser(userName);
		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null)
			return this.flowMgr.getFlowInfo(pe.getProjectName(), oc, instanceId);
		else
			return null;
	}
	
	@RequestMapping(value = "/instances/{instanceId}/log", method = RequestMethod.GET)
	String getFlowInstanceLog(@PathVariable String userName, @PathVariable String instanceId) {
		OozieConf oc = flowDeployer.getOozieServerConf();
		this.validateUser(userName);
		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null)
			return this.flowMgr.getFlowLog(pe.getProjectName(), oc, instanceId);
		else
			return null;
	}
	
	@RequestMapping(value = "/instances/{instanceId}/data/{dataName:.+}", method = RequestMethod.GET)
	InMemFile getFlowInstanceData(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String dataName) {
		this.validateUser(userName);
		OozieConf oc = flowDeployer.getOozieServerConf();
		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null) {
			FlowInfo flowInfo = this.flowMgr.getFlowInfo(pe.getProjectName(), oc, instanceId);
			if (flowInfo != null) {
				if (flowEntity != null && flowEntity.getJsonContent() != null && flowEntity.getJsonContent().length() > 0) {
					Flow flow = JsonUtil.fromJsonString(flowEntity.getJsonContent(), Flow.class);
					List<Data> datalist = flow.getData();
					if (datalist != null) {
						EngineConf ec = flowDeployer.getEngineConfig();
						for (Data d: datalist) {
							if (d != null && d.getName() != null && d.isInstance() && d.getName().equals(dataName)) {
								return this.flowMgr.getDFSFile(ec, d, flowInfo);
							}
						}
					}
					return null;
				} else {
					logger.debug("Flow {} not found", flowInfo.getName());
					return null;
				}
			} else {
				logger.debug("Flow instance {} not found", instanceId);
				return null;
			}
		} else {
			logger.debug("Project entity {} not found", flowEntity.getProjectId());
			return null;
		}
	}

	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}", method = RequestMethod.GET)
	NodeInfo getFlowNode(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOozieServerConf();
		this.validateUser(userName);
		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null)
			return this.flowMgr.getNodeInfo(pe.getProjectName(), oc, instanceId, nodeName);
		else
			return null;
	}
	
	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/log", method = RequestMethod.GET)
	InMemFile[] getFlowNodeLog(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOozieServerConf();
		this.validateUser(userName);
		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null)
			return this.flowMgr.getNodeLog(pe.getProjectName(), oc, instanceId, nodeName);
		else
			return null;
	}
	
	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/inputfiles", method = RequestMethod.GET)
	String[] listFlowNodeInputFiles(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOozieServerConf();
		EngineConf ec = flowDeployer.getEngineConfig();
		this.validateUser(userName);
		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null)
			return this.flowMgr.listNodeInputFiles(pe.getProjectName(), oc, ec, instanceId, nodeName);
		else
			return null;
	}

	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/outputfiles", method = RequestMethod.GET)
	String[] listFlowNodeOutputFiles(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOozieServerConf();
		EngineConf ec = flowDeployer.getEngineConfig();
		this.validateUser(userName);
		FlowInstanceEntity instance = this.flowInstanceRepository.getOne(instanceId);
		FlowEntity flowEntity = this.flowRepository.getOne(instance.getFlowName());
		ProjectEntity pe = this.projectRepository.getOne(flowEntity.getProjectId());
		if (pe != null)
			return this.flowMgr.listNodeOutputFiles(pe.getProjectName(), oc, ec, instanceId, nodeName);
		else
			return null;
	}

	@RequestMapping(value = "/dfs/**", method = RequestMethod.GET)
	InMemFile getDFSFile(@PathVariable String userName, HttpServletRequest request) {
		this.validateUser(userName);
		String filePath = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		EngineConf ec = flowDeployer.getEngineConfig();
		if (filePath != null && filePath.contains("/flow/dfs/"))
			filePath = filePath.substring(filePath.indexOf("/flow/dfs/") + 9);
		InMemFile f = this.flowMgr.getDFSFile(ec, filePath);
		if (f != null && f.getContent() != null) {
			/* Always return text data now */
			f.setTextContent(new String(f.getContent(), StandardCharsets.UTF_8));
			f.setContent(null);
			f.setFileType(FileType.textData);
		}
		return f;
	}

	@RequestMapping(value = "/dfs/**", method = { RequestMethod.PUT, RequestMethod.POST })
	boolean putDFSFile(@PathVariable String userName, HttpServletRequest request, @RequestBody String content) {
		this.validateUser(userName);
		String filePath = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		EngineConf ec = flowDeployer.getEngineConfig();
		if (filePath != null && filePath.contains("/flow/dfs/"))
			filePath = filePath.substring(filePath.indexOf("/flow/dfs/") + 9);
		InMemFile file = new InMemFile(FileType.textData, filePath, content);
		return flowMgr.putDFSFile(ec, filePath, file);
	}
	
	private void validateUser(String userName) {
		AccountEntity ae = this.accountRepository.findOne(userName);
		if (ae==null){
			throw new UserNotFoundException(userName);
		}
	}
	
}
