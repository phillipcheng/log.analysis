package bdap.schemagen;

import org.apache.commons.csv.CSVParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
	public static final Logger logger = LogManager.getLogger(Main.class);
	
	public static void main(String[] args) throws Exception {
		CSVParser parser = null;
		String dbSchema;
		String projectName;
		String projectPath;
		
		if (args.length > 1) {
			projectName = args[1];
			projectPath = "/" + projectName;
		} else {
			projectName = "";
			projectPath = "";
		}
			
		dbSchema = "mydb";
		
		if (args.length > 0) {
			
		} else {
			System.out.println("Usage: java -jar schema-generator-VVERSIONN.jar <path of schema definition csv file> [db schema]");
		}
	}
}
