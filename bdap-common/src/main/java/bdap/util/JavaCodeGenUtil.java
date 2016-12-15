package bdap.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class JavaCodeGenUtil {
	public static final Logger logger = LogManager.getLogger(JavaCodeGenUtil.class);
	
	public static String getVarName(String name){
		//check first digit
		if (Character.isDigit(name.charAt(0))){
			name = "_" + name;
		}
		//remove space and dot
		return name.replace("\\s", "_").replace(".", "_");
	}

}
