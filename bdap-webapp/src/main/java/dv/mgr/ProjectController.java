package dv.mgr;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import bdap.util.FileType;
import bdap.util.JsonUtil;
import dv.UserNotFoundException;
import dv.db.dao.AccountRepository;
import dv.db.dao.ProjectRepository;
import dv.db.entity.AccountEntity;
import dv.db.entity.ProjectEntity;
import etl.flow.Data;
import etl.flow.deploy.FlowDeployer;
import etl.flow.mgr.FlowMgr;
import etl.flow.mgr.InMemFile;

@RestController
@RequestMapping("/{userName}/project")
public class ProjectController {
	public static final Logger logger = LogManager.getLogger(FlowController.class);
	private final AccountRepository accountRepository;
	private final ProjectRepository projectRepository;
	private final FlowMgr flowMgr;
	private final FlowDeployer flowDeployer;
	
	@Autowired
	ProjectController(ProjectRepository projectRepository, AccountRepository accountRepository, FlowMgr flowMgr, FlowDeployer flowDeployer) {
		this.accountRepository = accountRepository;
		this.projectRepository = projectRepository;
		this.flowDeployer = flowDeployer;
		this.flowMgr = flowMgr;
	}
	
	@RequestMapping(value = "/", method = RequestMethod.GET)
	List<ProjectEntity> listProjects(@PathVariable String userName) {
		this.validateUser(userName);
		return this.projectRepository.findAll();
	}
	
	@RequestMapping(value = "/{projectName}", method = RequestMethod.DELETE)
	 void deleteProject(@PathVariable String userName, @PathVariable String projectName) {
		this.validateUser(userName);
		this.projectRepository.deleteByName(projectName);
	}
	
	@RequestMapping(method = RequestMethod.POST)
	ResponseEntity<?> add(@PathVariable String userName, @RequestBody Map<String, String> input) {
		this.validateUser(userName);
		
		logger.debug(input);
		
		ProjectEntity pe = new ProjectEntity();
		pe.setProjectName(input.get("projectName"));
		pe.setType(input.get("type"));
		pe.setUpdateTime(new Date(System.currentTimeMillis()));
		pe.setContent(JsonUtil.toJsonString(input));
		pe = projectRepository.save(pe);

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.setLocation(ServletUriComponentsBuilder
				.fromCurrentRequest().path("/{projectName}")
				.buildAndExpand(pe.getProjectName()).toUri());
		return new ResponseEntity<>(null, httpHeaders, HttpStatus.CREATED);
	}
	
	@RequestMapping(value = "/{projectName}", method = RequestMethod.GET)
	ProjectEntity getProject(@PathVariable String userName, @PathVariable String projectName) {
		this.validateUser(userName);
		return this.projectRepository.findByName(projectName);
	}
	
	@RequestMapping(value = "/{projectName}/dir/**", method = { RequestMethod.PUT, RequestMethod.POST })
	boolean putFile(@PathVariable String userName, @PathVariable String projectName, HttpServletRequest request, @RequestParam("uploadFile") MultipartFile file) throws IOException {
		this.validateUser(userName);
		String filePath = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		if (filePath != null) {
			if (filePath.contains("/dir"))
				filePath = filePath.substring(filePath.indexOf("/dir") + 4);
		} else {
			filePath = "";
		}
		ProjectEntity pe = this.projectRepository.findByName(projectName);
		String hdfsDir = JsonUtil.fromJsonString(pe.getContent(), "hdfsDir", String.class);
		if (hdfsDir != null) {
			if (!hdfsDir.endsWith(Path.SEPARATOR))
				hdfsDir += Path.SEPARATOR;
			if (filePath.startsWith(Path.SEPARATOR))
				filePath = filePath.substring(1);
			filePath = hdfsDir + filePath;
		}
		
		String of = file.getOriginalFilename();
		if (of != null) {
			of = of.substring(of.lastIndexOf("/") + 1);
			of = of.substring(of.lastIndexOf("\\") + 1);
		} else {
			of = "";
		}
		
		if (!filePath.endsWith(of)) {
			if (filePath.endsWith(Path.SEPARATOR))
				filePath = filePath + of;
			else
				filePath = filePath + Path.SEPARATOR + of;
		}
		
		InMemFile f = new InMemFile(FileType.binaryData, filePath, file.getBytes());
		return flowMgr.putDFSFile(flowDeployer.getEngineConfig(), filePath, f);
	}
	
	@RequestMapping(value = "/{projectName}/dir/**", method = RequestMethod.GET)
	List<Map<String, String>> listDir(@PathVariable String userName, @PathVariable String projectName, HttpServletRequest request) {
		this.validateUser(userName);
		String filePath = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		if (filePath != null) {
			if (filePath.contains("/dir"))
				filePath = filePath.substring(filePath.indexOf("/dir") + 4);
		} else {
			filePath = "";
		}
		ProjectEntity pe = this.projectRepository.findByName(projectName);
		String hdfsDir = JsonUtil.fromJsonString(pe.getContent(), "hdfsDir", String.class);
		if (hdfsDir != null) {
			if (!hdfsDir.endsWith(Path.SEPARATOR))
				hdfsDir += Path.SEPARATOR;
			if (filePath.endsWith(Path.SEPARATOR))
				filePath = filePath.substring(0, filePath.length() - 1);
			filePath = hdfsDir + filePath;
		} else {
			hdfsDir = Path.SEPARATOR;
		}
    	String logDirPath = hdfsDir + "logs" + Path.SEPARATOR;
		Data data = new Data();
		data.setName("ProjectData");
		data.setInstance(false);
		data.setLocation(filePath);
		return dirList(this.flowMgr.getDFSFile(flowDeployer.getEngineConfig(), data), logDirPath);
	}
	
	private List<Map<String, String>> dirList(InMemFile dfsFile, String logDirPath) {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		if (dfsFile != null && FileType.directoryList.equals(dfsFile.getFileType())) {
			if (dfsFile.getTextContent() != null) {
				String[] filenames = dfsFile.getTextContent().split("\\n");
				if (filenames != null) {
					Map<String, String> fileInfo;
					for (String f: filenames) {
						if (f != null && !f.startsWith(logDirPath)) {
							/* Filter the log files */
							fileInfo = new HashMap<String, String>();
							fileInfo.put("fileType", "Normal");
							fileInfo.put("fileName", f);
							list.add(fileInfo);
						}
					}
				}
			}
		}
		return list;
	}

	private void validateUser(String userName) {
		AccountEntity ae = this.accountRepository.findOne(userName);
		if (ae==null){
			throw new UserNotFoundException(userName);
		}
	}
}
