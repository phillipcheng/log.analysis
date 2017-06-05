package etl.telecom;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TelecomUtil {

	public static final Logger logger = LogManager.getLogger(TelecomUtil.class);
	
	private static CountryCode ccMap = new CountryCode();
	private static NanpaCode nanpaMap = new NanpaCode();
	
	public static String processE164(String value){
		//append country code
		String output ="";
		String cc = null;
		if (value!=null && !"".equals(value.trim())){
			cc = ccMap.getCode(value);
			if (cc!=null){
				output+=cc;
			}else{
				logger.error(String.format("country code not found for %s", value));
			}
		}
		output+=",";
		//append nanpa code
		if (cc!=null && "1".equals(cc)){
			value = value.substring(cc.length());
			if (value.length()>6){
				String str1 = value.substring(0, 3);
				String str2 = value.substring(3,6);
				String nanpaCode = str1 + "-" + str2;
				if (nanpaMap.hasNanpa(nanpaCode)){
					output+=nanpaCode;
				}else{
					logger.error("nanpaCode not found for:" + value);
				}
			}else{
				logger.error("nanpaCode not found for:" + value);
			}
		}
		return output;
	}
	
	public static String processGTAddress(String value){
		String output ="";
		if (value!=null && !"".equals(value.trim())){
			String cc = ccMap.getCode(value);
			if (cc!=null){
				output+=cc;
			}else{
				logger.error(String.format("country code not found for %s", value));
			}
		}
		return output;
	}
}
