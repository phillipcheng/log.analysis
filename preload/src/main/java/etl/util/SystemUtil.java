package etl.util;

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
}
