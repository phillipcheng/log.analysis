package dv.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.util.HdfsClassLoader;
import bdap.util.HdfsUtil;
import etl.flow.deploy.DefaultDeployMethod;
import etl.flow.deploy.DeployMethod;

public class AppProjectClassLoader extends HdfsClassLoader {
	public static final Logger logger = LogManager.getLogger(AppProjectClassLoader.class);
	
	public AppProjectClassLoader(String defaultFs, DeployMethod deployMethod, String libpath) {
		super(deployMethod instanceof DefaultDeployMethod ? ((DefaultDeployMethod) deployMethod).getFs() : HdfsUtil.getHadoopFs(defaultFs), libpath);
	}

}
