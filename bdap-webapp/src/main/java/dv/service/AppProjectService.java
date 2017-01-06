package dv.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import bdap.util.JsonUtil;
import dv.db.dao.ProjectRepository;
import dv.db.entity.ProjectEntity;
import etl.flow.deploy.ProjectService;

public class AppProjectService implements ProjectService {
	
	private ProjectRepository projectRepository;
	
	public String getHdfsDir(String prjName) {
		ProjectEntity pe = projectRepository.findByName(prjName);
		if (pe != null) {
			String content = pe.getContent();
			return JsonUtil.fromJsonString(content, "hdfsDir", String.class);
		} else
			return null;
	}
	
	/* The local dir is unneeded in webapp */
	public String getLocalDir(String prjName) {
		return "";
	}
	
	public Collection<String> listProjects() {
		List<ProjectEntity> projects = projectRepository.findAll();
		List<String> projectNames = new ArrayList<String>();
		if (projects != null)
			for (ProjectEntity pe: projects)
				projectNames.add(pe.getProjectName());
		return projectNames;
	}

	public void setProjectRepository(ProjectRepository projectRepository) {
		this.projectRepository = projectRepository;
	}

}
