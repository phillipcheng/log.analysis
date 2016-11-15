package bdap.tools.pushagent;

public class Element {
	/* PK */ private String name;
	/* PK */ private String hostname;
	private String ip;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public String getIp() {
		return ip;
	}
	public String getIP() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
}
