package bdap.util;

public class SftpInfo{
	protected String user;
	protected String passwd;
	protected String ip;
	protected int port;
	
	public SftpInfo(String user, String passwd, String ip, int port){
		this.user = user;
		this.passwd = passwd;
		this.ip = ip;
		this.port = port;
	}
}
