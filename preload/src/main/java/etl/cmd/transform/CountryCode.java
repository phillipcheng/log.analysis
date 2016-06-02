package etl.cmd.transform;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

public class CountryCode {
	
	private static Set<String> ccSet = new HashSet<String>();
	public static final Logger logger = Logger.getLogger(CountryCode.class);
	
	public void init(String fileName){
		BufferedReader br=null;
		String line;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			while ((line = br.readLine()) != null) {
			    String[] vals = line.split(",");
			    ccSet.add(vals[vals.length-1]);
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
