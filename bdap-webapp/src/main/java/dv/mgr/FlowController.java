package dv.mgr;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import bdap.util.EngineConf;
import bdap.util.JsonUtil;
import bdap.util.PropertiesUtil;
import dv.UserNotFoundException;
import dv.db.dao.AccountRepository;
import dv.db.dao.FlowRepository;
import dv.db.entity.AccountEntity;
import dv.db.entity.FlowEntity;
import etl.engine.ETLCmd;
import etl.flow.Flow;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;
import etl.flow.oozie.OozieConf;

@RestController
@RequestMapping("/{userName}/flow")
public class FlowController {
	public static final Logger logger = LogManager.getLogger(FlowController.class);
	private static final String config = "config/config.properties";
	private static final String[] SEARCH_PACKAGES = new String[] {"etl.cmd"};
	private static final String ACTION_NODE_CMDS_SEARCH_PACKAGES = "action.node.cmds.search.packages";
	private static final String CMD_PARAMETER_PREFIX = "cfgkey_";
	private String[] searchPackages;
	
	@Autowired
	FlowController(FlowRepository flowRepository, AccountRepository accountRepository, FlowMgr flowMgr, FlowDeployer flowDeployer) {
		this.flowRepository = flowRepository;
		this.accountRepository = accountRepository;
		this.flowMgr = flowMgr;
		this.flowDeployer = flowDeployer;
		
		PropertiesConfiguration pc = PropertiesUtil.getPropertiesConfig(config);
		this.searchPackages = pc.getStringArray(ACTION_NODE_CMDS_SEARCH_PACKAGES);
		if (searchPackages == null || searchPackages.length == 0)
			searchPackages = SEARCH_PACKAGES;
	}
	
	private final FlowRepository flowRepository;
	private final AccountRepository accountRepository;
	private final FlowMgr flowMgr;
	private final FlowDeployer flowDeployer;

	@RequestMapping(value = "/node/types/action/commands", method = RequestMethod.GET)
	Map<String, List<String>> getActionNodeCommands() {
		ClassPathScanningCandidateComponentProvider actionsProvider = new ClassPathScanningCandidateComponentProvider(false);
		actionsProvider.addIncludeFilter(new AssignableTypeFilter(ETLCmd.class));
		Map<String, List<String>> classNames = new HashMap<String, List<String>>();
		List<String> cmdParameters;
		Set<BeanDefinition> beanDefs;
		Class<?> cls;
		Field[] fields;
		Object obj;
		for (String pkgPath: searchPackages) {
			beanDefs = actionsProvider.findCandidateComponents(pkgPath);
			for (BeanDefinition def: beanDefs) {
				logger.info(def.getBeanClassName());
				cmdParameters = new ArrayList<String>();
				try {
					cls = Class.forName(def.getBeanClassName());
					fields = cls.getFields();
					if (fields != null) {
						for (Field f: fields) {
						    if (Modifier.isStatic(f.getModifiers()) && f.getName().startsWith(CMD_PARAMETER_PREFIX)) {
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
				} catch (ClassNotFoundException e) {
					logger.error(e.getMessage(), e);
				}
				classNames.put(def.getBeanClassName(), cmdParameters);
			}
		}
		return classNames;
	}
	
	@RequestMapping(method = RequestMethod.POST)
	ResponseEntity<?> add(@PathVariable String userName, @RequestBody Flow input) {
		this.validateUser(userName);
		FlowEntity fe = flowRepository.save(new FlowEntity(input.getName(),
				userName, JsonUtil.toJsonString(input)));

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
	
	@RequestMapping(value = "/{flowId}", method = RequestMethod.POST)
	ResponseEntity<?> execute(@PathVariable String userName, @PathVariable String flowId) {
		HttpHeaders httpHeaders = new HttpHeaders();
		OozieConf oc = flowDeployer.getOC();
		EngineConf ec = flowDeployer.getEC();
		this.validateUser(userName);
		try {
			FlowEntity flowEntity = flowRepository.findOne(flowId);
			if (flowEntity != null && flowEntity.getJsonContent() != null && flowEntity.getJsonContent().length() > 0) {
				Flow flow = JsonUtil.fromJsonString(flowEntity.getJsonContent(), Flow.class);
				flowMgr.deployFlowFromJson("project1", flow, flowDeployer, oc, ec);
				String flowInstanceId = flowMgr.executeFlow("project1", flowId, oc, ec);
				return new ResponseEntity<String>(flowInstanceId, httpHeaders, HttpStatus.CREATED);
			} else {
				return new ResponseEntity<String>("Flow entity not found or is empty", httpHeaders, HttpStatus.NOT_FOUND);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return new ResponseEntity<String>(e.getMessage(), httpHeaders, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@RequestMapping(value = "/instances/{instanceId}", method = RequestMethod.GET)
	FlowInfo getFlowInstance(@PathVariable String userName, @PathVariable String instanceId) {
		OozieConf oc = flowDeployer.getOC();
		this.validateUser(userName);
		return this.flowMgr.getFlowInfo("project1", oc, instanceId);
	}
	
	@RequestMapping(value = "/instances/{instanceId}/log", method = RequestMethod.GET)
	String getFlowLog(@PathVariable String userName, @PathVariable String instanceId) {
		OozieConf oc = flowDeployer.getOC();
		this.validateUser(userName);
		return this.flowMgr.getFlowLog("project1", oc, instanceId);
	}

	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}", method = RequestMethod.GET)
	NodeInfo getFlowNode(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOC();
		this.validateUser(userName);
		return this.flowMgr.getNodeInfo("project1", oc, instanceId, nodeName);
	}
	
	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/log", method = RequestMethod.GET)
	InMemFile[] getFlowNodeLog(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOC();
		this.validateUser(userName);
		return this.flowMgr.getNodeLog("project1", oc, instanceId, nodeName);
	}
	
	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/inputfiles", method = RequestMethod.GET)
	String[] listFlowNodeInputFiles(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOC();
		EngineConf ec = flowDeployer.getEC();
		this.validateUser(userName);
		return this.flowMgr.listNodeInputFiles("project1", oc, ec, instanceId, nodeName);
	}

	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/outputfiles", method = RequestMethod.GET)
	String[] listFlowNodeOutputFiles(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		OozieConf oc = flowDeployer.getOC();
		EngineConf ec = flowDeployer.getEC();
		this.validateUser(userName);
		return this.flowMgr.listNodeOutputFiles("project1", oc, ec, instanceId, nodeName);
	}

	@RequestMapping(value = "/dfs/**", method = RequestMethod.GET)
	InMemFile getDFSFile(@PathVariable String userName, HttpServletRequest request) {
		String filePath = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		EngineConf ec = flowDeployer.getEC();
		if (filePath != null && filePath.contains("/flow/dfs/"))
			filePath = filePath.substring(filePath.indexOf("/flow/dfs/") + 9);
		this.validateUser(userName);
		return this.flowMgr.getDFSFile(ec, filePath);
	}
	
	private void validateUser(String userName) {
		AccountEntity ae = this.accountRepository.findOne(userName);
		if (ae==null){
			throw new UserNotFoundException(userName);
		}
	}
	
}
