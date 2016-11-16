package etl.telecom;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

//log4j2
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CountryCode {
	public static final Logger logger = LogManager.getLogger(CountryCode.class);
	public static final String cc_file = "iso_3166_2_countries.csv";
	
	private static Set<String> ccSet = new HashSet<String>();
	
	public CountryCode(){
		init(cc_file);
	}
	
	public void init(String fileName){
		BufferedReader br=null;
		String line;
		try {
			br = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(fileName)));
			while ((line = br.readLine()) != null) {
			    String[] vals = line.split(",");
			    ccSet.add(vals[vals.length-5]);
			}
		}catch (Exception e){
			logger.error("", e);
		}finally{
			try {
				br.close();
			}catch(Exception e){
				logger.error("", e);
			}
		}
	}
	
	//return the cc code for the e164 number, null if not found
	public String getCode(String e164){
		for (int i=1; i<=4; i++){
			if (e164.length()>=i){
				String prefix = e164.substring(0, i);
				if (ccSet.contains(prefix)){
					return prefix;
				}
			}else{
				break;
			}
		}
		return null;
	}
}
