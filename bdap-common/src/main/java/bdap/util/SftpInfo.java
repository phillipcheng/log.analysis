package bdap.util;

public class SftpInfo{
	String user;
	String passwd;
	String ip;
	int port;
	
	public SftpInfo(String user, String passwd, String ip, int port){
		this.user = user;
		this.passwd = passwd;
		this.ip = ip;
		this.port = port;
	}
}