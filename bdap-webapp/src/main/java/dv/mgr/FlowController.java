package dv.mgr;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
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
import dv.UserNotFoundException;
import dv.db.dao.AccountRepository;
import dv.db.dao.FlowRepository;
import dv.db.entity.AccountEntity;
import dv.db.entity.FlowEntity;
import etl.flow.Flow;
import etl.flow.mgr.FlowInfo;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;
import etl.flow.mgr.NodeInfo;
import etl.flow.oozie.OozieConf;

@RestController
@RequestMapping("/{userName}/flow")
public class FlowController {
	
	@Autowired
	FlowController(FlowRepository flowRepository, AccountRepository accountRepository, FlowMgr flowMgr) {
		this.flowRepository = flowRepository;
		this.accountRepository = accountRepository;
		this.flowMgr = flowMgr;
	}
	
	private final FlowRepository flowRepository;
	private final AccountRepository accountRepository;
	private final FlowMgr flowMgr;
	
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
	
	@RequestMapping(value = "/instances/{instanceId}", method = RequestMethod.GET)
	FlowInfo getFlowInstance(@PathVariable String userName, @PathVariable String instanceId) {
		this.validateUser(userName);
		OozieConf oc = new OozieConf("127.0.0.1", 11000, "hdfs://127.0.0.1:19000", "127.0.0.1:8032", "");
		oc.setOozieLibPath(String.format("%s/user/%s/share/lib/preload/lib/", oc.getNameNode(), oc.getUserName()));
		oc.setHistoryServer("http://localhost:19888");
		oc.setRmWebApp("http://localhost:8088");
		return this.flowMgr.getFlowInfo("project1", oc, instanceId);
	}
	
	@RequestMapping(value = "/instances/{instanceId}/log", method = RequestMethod.GET)
	String getFlowLog(@PathVariable String userName, @PathVariable String instanceId) {
		this.validateUser(userName);
		OozieConf oc = new OozieConf("127.0.0.1", 11000, "hdfs://127.0.0.1:19000", "127.0.0.1:8032", "");
		oc.setOozieLibPath(String.format("%s/user/%s/share/lib/preload/lib/", oc.getNameNode(), oc.getUserName()));
		oc.setHistoryServer("http://localhost:19888");
		oc.setRmWebApp("http://localhost:8088");
		return this.flowMgr.getFlowLog("project1", oc, instanceId);
	}

	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}", method = RequestMethod.GET)
	NodeInfo getFlowNode(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		this.validateUser(userName);
		OozieConf oc = new OozieConf("127.0.0.1", 11000, "hdfs://127.0.0.1:19000", "127.0.0.1:8032", "");
		oc.setOozieLibPath(String.format("%s/user/%s/share/lib/preload/lib/", oc.getNameNode(), oc.getUserName()));
		oc.setHistoryServer("http://localhost:19888");
		oc.setRmWebApp("http://localhost:8088");
		return this.flowMgr.getNodeInfo("project1", oc, instanceId, nodeName);
	}
	
	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/log", method = RequestMethod.GET)
	String getFlowNodeLog(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		this.validateUser(userName);
		OozieConf oc = new OozieConf("127.0.0.1", 11000, "hdfs://127.0.0.1:19000", "127.0.0.1:8032", "");
		oc.setOozieLibPath(String.format("%s/user/%s/share/lib/preload/lib/", oc.getNameNode(), oc.getUserName()));
		oc.setHistoryServer("http://localhost:19888");
		oc.setRmWebApp("http://localhost:8088");
		return this.flowMgr.getNodeLog("project1", oc, instanceId, nodeName);
	}
	
	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/inputfiles", method = RequestMethod.GET)
	String[] listFlowNodeInputFiles(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		this.validateUser(userName);
		OozieConf oc = new OozieConf("127.0.0.1", 11000, "hdfs://127.0.0.1:19000", "127.0.0.1:8032", "");
		oc.setOozieLibPath(String.format("%s/user/%s/share/lib/preload/lib/", oc.getNameNode(), oc.getUserName()));
		oc.setHistoryServer("http://localhost:19888");
		oc.setRmWebApp("http://localhost:8088");
		EngineConf ec = new EngineConf();
		ec.getPropertiesConf().setProperty(EngineConf.cfgkey_defaultFs, "hdfs://127.0.0.1:19000");
		return this.flowMgr.listNodeInputFiles("project1", oc, ec, instanceId, nodeName);
	}

	@RequestMapping(value = "/instances/{instanceId}/nodes/{nodeName}/outputfiles", method = RequestMethod.GET)
	String[] listFlowNodeOutputFiles(@PathVariable String userName, @PathVariable String instanceId, @PathVariable String nodeName) {
		this.validateUser(userName);
		OozieConf oc = new OozieConf("127.0.0.1", 11000, "hdfs://127.0.0.1:19000", "127.0.0.1:8032", "");
		oc.setOozieLibPath(String.format("%s/user/%s/share/lib/preload/lib/", oc.getNameNode(), oc.getUserName()));
		oc.setHistoryServer("http://localhost:19888");
		oc.setRmWebApp("http://localhost:8088");
		EngineConf ec = new EngineConf();
		ec.getPropertiesConf().setProperty(EngineConf.cfgkey_defaultFs, "hdfs://127.0.0.1:19000");
		return this.flowMgr.listNodeOutputFiles("project1", oc, ec, instanceId, nodeName);
	}

	@RequestMapping(value = "/dfs/**", method = RequestMethod.GET)
	InMemFile getDFSFile(@PathVariable String userName, HttpServletRequest request) {
		String filePath = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		EngineConf ec = new EngineConf();
		ec.getPropertiesConf().setProperty(EngineConf.cfgkey_defaultFs, "hdfs://127.0.0.1:19000");
		if (filePath != null && filePath.contains("/flow/dfs/"))
			filePath = filePath.substring(filePath.indexOf("/flow/dfs/") + 9);
		return this.flowMgr.getDFSFile(ec, filePath);
	}
	
	private void validateUser(String userName) {
		AccountEntity ae = this.accountRepository.findOne(userName);
		if (ae==null){
			throw new UserNotFoundException(userName);
		}
	}
	
}
