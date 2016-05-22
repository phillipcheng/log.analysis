package hpe.mtc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class ETLCmd {
	public static final Logger logger = Logger.getLogger(ETLCmd.class);
	/*
	 * 
	 * -- sftp processor --
	 * detect new files coming in the folder, sftp them to our input dir
	 * 
	 * -- xml processor --
	 * if schema file not found (initial running)
	 * 		check all input files to generate schema, table.sql and copy.sql under schema-folder
	 * else
	 *		check all input files to see any schema update needed
	 * 		if new attributes detected
	 * 			generate: schema, alter-table.sql, create-table.sql and copy.sql
	 * 			backing up: old schema, create-table.sql and copy.sql to history folder with timestamp.
	 * generate csv files according to the latest schema to output-data-folder
	 * after processing move these input xml files to data-lake
	 * 
	 * -- csv processor --
	 * if there is update-table sql, update the db schema
	 * else if there is create-table sql, 1st time running, create the db schema
	 * copy the csv files to db
	 * after processing move these csv files to data-lake
	 */
	public static void usage(){
		System.out.println("ETLCmd -xmld xml-folder -csvd csv-folder -sd schema-folder -sh schema-history-folder -dh data-history-folder -p prefix");
	}
	public static void main(String[] args){
		CommandLine commandLine;
		Option optionCfg = Option.builder("c").hasArgs().required(true).longOpt("config-file").build();
		Option optionInput = Option.builder("x").hasArgs().longOpt("xml-folder").build();
		Option optionPrefix = Option.builder("p").hasArgs().longOpt("prefix").build();
		Options options = new Options();
		options.addOption(optionCfg);
		options.addOption(optionInput);
		options.addOption(optionPrefix);
		CommandLineParser parser = new DefaultParser();
		try{
			commandLine = parser.parse(options, args);
			String cfg = commandLine.getOptionValue("c");
			PropertiesConfiguration pc = Util.getPropertiesConfig(cfg);
			pc.setAutoSave(false);
			if (commandLine.hasOption("x")){
				String xmlFolder = commandLine.getOptionValue("x");
				pc.setProperty(XpathProcessor.cfgkey_xml_folder, xmlFolder);
			}
			if (commandLine.hasOption("p")){
				String prefix = commandLine.getOptionValue("p");
				pc.setProperty(XpathProcessor.cfgkey_prefix, prefix);
			}
			//XmlProcessor processor = new XmlProcessor(pc);
			XpathProcessor processor = new XpathProcessor(pc);
			processor.process();
			
		}catch (ParseException exception){
			logger.info(exception);
			usage();
		}
	}
}
