package etl.flow.deploy;

import java.util.List;

public interface DeployMethod {
	public static final String FTP = "ftp";
	public static final String SSH = "ssh";
	public static final String DEFAULT = "default";
	
	public void createFile(String remotePath, byte[] content);
	public void delete(String remotePath, boolean recursive);
	public void copyFromLocalFile(String localPath, String remotePath);
	public void copyFromLocalFile(boolean delSrc, boolean overwrite, String localPath, String remotePath);
	public List<String> listFiles(String path);
	public List<String> readFile(String path);
}
