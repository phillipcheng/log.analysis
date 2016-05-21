package hpe.mtc;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class ETLCmd {
	public static final Logger logger = Logger.getLogger(ETLCmd.class);
	/*
	 * 
	 * -- sftp processor --
	 * detect new files coming in the folder, sftp them to our input dir
	 * 
	 * -- xml processor --
	 * if schema file not found
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
		Option optionInput = Option.builder("x").hasArgs().required(true).longOpt("xml-folder").build();
		Option optionOutput = Option.builder("c").hasArgs().required(true).longOpt("csv-folder").build();
		Option optionSchema = Option.builder("s").hasArgs().required(true).longOpt("schema-folder").build();
		Option optionSchemaHistory = Option.builder("m").hasArgs().required(true).longOpt("schema-history-folder").build();
		Option optionDataHistory = Option.builder("d").hasArgs().required(true).longOpt("data-history-folder").build();
		Option optionPrefix = Option.builder("p").hasArgs().required(true).longOpt("prefix").build();
		Options options = new Options();
		options.addOption(optionInput);
		options.addOption(optionOutput);
		options.addOption(optionSchema);
		options.addOption(optionSchemaHistory);
		options.addOption(optionDataHistory);
		options.addOption(optionPrefix);
		CommandLineParser parser = new DefaultParser();
		try{
			commandLine = parser.parse(options, args);
			String xmlFolder = commandLine.getOptionValue("x");
			String csvFolder = commandLine.getOptionValue("c");
			String schemaFolder = commandLine.getOptionValue("s");
			String schemaHistoryFolder = commandLine.getOptionValue("m");
			String dataHistoryFolder = commandLine.getOptionValue("d");
			
			String prefix = null;
			if (commandLine.hasOption("p")){
				prefix = commandLine.getOptionValue("p");
			}
			XmlProcessor xmlProcessor = new XmlProcessor(xmlFolder, csvFolder, schemaFolder, schemaHistoryFolder, dataHistoryFolder, prefix);
			xmlProcessor.process();
			
		}catch (ParseException exception){
			logger.info(exception);
			usage();
		}
	}
}
