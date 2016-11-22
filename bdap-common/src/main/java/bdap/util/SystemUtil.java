package bdap.util;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class SystemUtil {

	public static final Logger logger = LogManager.getLogger(SystemUtil.class);
	
	public static OSType getOsType(){
		String os = System.getProperty("os.name").toLowerCase();
		logger.info(String.format("osName:%s", os));
		if (os.indexOf("win")>=0){
			return OSType.WINDOWS;
		}else if (os.indexOf("mac") >= 0){
			return OSType.MAC;
		}else if (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") > 0){
			return OSType.UNIX;
		}else{
			logger.error(String.format("ostype:%s not recognized, treat as unix.", os));
			return OSType.UNIX;
		}
	}
	
	public static Set<String> getMyIpAddresses(){
		Set<String> ipAddresses = new HashSet<String>();
		ipAddresses.add("127.0.0.1");//always has loop back
		try {
			InetAddress localhost = InetAddress.getLocalHost();
			logger.info(" IP Addr: " + localhost.getHostAddress());
			// Just in case this host has multiple IP addresses....
			InetAddress[] allMyIps = InetAddress.getAllByName(localhost.getCanonicalHostName());
			if (allMyIps != null && allMyIps.length > 1) {
				for (int i = 0; i < allMyIps.length; i++) {
					logger.info("    " + allMyIps[i]);
					ipAddresses.add(allMyIps[i].getHostAddress().toString());
				}
			}
		} catch (UnknownHostException e) {
			System.out.println(" (error retrieving server host name)");
		}
		return ipAddresses;
	}
	
	public static String execCmd(String cmd) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
		DefaultExecutor executor = new DefaultExecutor();
		executor.setStreamHandler(streamHandler);
		CommandLine cmdLine = CommandLine.parse(cmd);
		try {
			executor.execute(cmdLine);
		}catch(Exception e){
			logger.error("", e);
		}
		String ret = outputStream.toString();
		return ret;
	}
}
