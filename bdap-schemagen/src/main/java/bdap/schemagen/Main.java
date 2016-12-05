package bdap.schemagen;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import bdap.schemagen.config.Config;
import bdap.schemagen.datamodel.GenLogicSchema;
import bdap.util.JsonUtil;
import etl.engine.LogicSchema;
import etl.util.DBType;
import etl.util.SchemaUtils;

public class Main {
	public static final Logger logger = LogManager.getLogger(Main.class);
	private static final String DEFAULT_DB_SCHEMA = "mydb";

	public static void main(String[] args) throws Exception {
		InputStream in = null;
		BufferedReader reader = null;
		BufferedWriter writer = null;
		Options options = new Options();
		Option opt = Option.builder("gen").hasArg(true).desc("Generate the schema").build();
		options.addOption(opt);
		opt = Option.builder("multiple").hasArg(false).desc("Whether to generate multiple json files (one json file per table)").build();
		options.addOption(opt);
		opt = Option.builder("final").hasArg(false).desc("Generate the final json files").build();
		options.addOption(opt);
		opt = Option.builder("to").hasArg(true).desc("Specify the new path of created schema").build();
		options.addOption(opt);
		opt = Option.builder("from").hasArg(true).desc("CSV file represents the schema / schema file").build();
		options.addOption(opt);
		opt = Option.builder("by").hasArg(true).desc("config json file").build();
		options.addOption(opt);
		opt = Option.builder("join").hasArg(true).desc("Join the schema with another").build();
		options.addOption(opt);
		opt = Option.builder("with").hasArg(true).desc("Append/join with the schema json file").build();
		options.addOption(opt);
		opt = Option.builder("append").hasArg(true).desc("Append the schema with the common schema").build();
		options.addOption(opt);
		opt = Option.builder("gensql").hasArg(true).desc("Generate the SQL file from the schema").build();
		options.addOption(opt);
		opt = Option.builder("dbschema").hasArg(true).desc("DB schema name").build();
		options.addOption(opt);
		opt = Option.builder("genfiletablemapping").hasArg(true).desc("Generate the file table mapping file from the schema").build();
		options.addOption(opt);
		opt = Option.builder("help").hasArg(false).desc("display the usage help").build();
		options.addOption(opt);

		CommandLineParser parser = new DefaultParser();
		// parse the command line arguments
		CommandLine line = parser.parse(options, args);

		if (line.hasOption("help")) {
			HelpFormatter formatter = new HelpFormatter();
			// formatter.printHelp(new PrintWriter(System.out), 0,
			// "schema-generator", "schema-generator", options, 0, 0, "");
			StringBuilder syntax = new StringBuilder();
			syntax.append("java -jar schema-generator.jar [-final] [-multiple] -gen <path> -from <path> -by <path>\n");
			syntax.append("java -jar schema-generator.jar [-final] [-multiple] -join <path> -with <path> [-to <path>]\n");
			syntax.append("java -jar schema-generator.jar [-final] [-multiple] -append <path> -with <path> [-to <path>]\n");
			syntax.append("java -jar schema-generator.jar -gensql <path> -from <path> [-dbschema <schema-name>]\n");
			syntax.append("java -jar schema-generator.jar -genfiletablemapping <path> -from <path>\n");
			formatter.printHelp(syntax.toString(), options);
			
		} else if (line.hasOption("gen")) try {
			Config config = (Config) JsonUtil.fromLocalJsonFile(line.getOptionValue("by"), Config.class);
			CSVSchemaGenerator g = new CSVSchemaGenerator();
			in = new FileInputStream(line.getOptionValue("from"));
			reader = new BufferedReader(new InputStreamReader(in, "utf8"));
			LogicSchema schema = g.generate(reader, config);
			
			Map<String, List<String>> attrIdMap = ((GenLogicSchema) schema).getAttrIdMap();
			if (line.hasOption("multiple"))
				((GenLogicSchema) schema).setAttrIdMap(Collections.emptyMap());
			
			if (line.hasOption("final")) {
				SchemaUtils.toLocalJsonPath(line.getOptionValue("gen"), line.hasOption("multiple"), createFinalLogicSchema(schema), attrIdMap);
			} else {
				SchemaUtils.toLocalJsonPath(line.getOptionValue("gen"), line.hasOption("multiple"), schema, attrIdMap);
			}
			
		} finally {
			if (reader != null)
				reader.close();
			if (in != null)
				in.close();
			
		} else if (line.hasOption("join")) {
			LogicSchema schema = SchemaUtils.fromLocalJsonPath(line.getOptionValue("join"), GenLogicSchema.class);
			LogicSchema additionalSchema = SchemaUtils.fromLocalJsonPath(line.getOptionValue("with"), GenLogicSchema.class);
			CSVSchemaGenerator g = new CSVSchemaGenerator();
			schema = g.outerJoinSchema(schema, additionalSchema);
			String schemaPath;
			if (line.hasOption("to"))
				schemaPath = line.getOptionValue("to");
			else
				schemaPath = line.getOptionValue("join");

			Map<String, List<String>> attrIdMap = ((GenLogicSchema) schema).getAttrIdMap();
			if (line.hasOption("multiple"))
				((GenLogicSchema) schema).setAttrIdMap(Collections.emptyMap());
			
			if (line.hasOption("final")) {
				SchemaUtils.toLocalJsonPath(schemaPath, line.hasOption("multiple"), createFinalLogicSchema(schema), attrIdMap);
			} else {
				SchemaUtils.toLocalJsonPath(schemaPath, line.hasOption("multiple"), schema, attrIdMap);
			}
			
		} else if (line.hasOption("append")) {
			LogicSchema schema = SchemaUtils.fromLocalJsonPath(line.getOptionValue("append"), GenLogicSchema.class);
			LogicSchema commonSchema = SchemaUtils.fromLocalJsonPath(line.getOptionValue("with"), GenLogicSchema.class);
			CSVSchemaGenerator g = new CSVSchemaGenerator();
			schema = g.insertSchema(schema, commonSchema);
			String schemaPath;
			if (line.hasOption("to"))
				schemaPath = line.getOptionValue("to");
			else
				schemaPath = line.getOptionValue("append");

			Map<String, List<String>> attrIdMap = ((GenLogicSchema) schema).getAttrIdMap();
			if (line.hasOption("multiple"))
				((GenLogicSchema) schema).setAttrIdMap(Collections.emptyMap());
			
			if (line.hasOption("final")) {
				SchemaUtils.toLocalJsonPath(schemaPath, line.hasOption("multiple"), createFinalLogicSchema(schema), attrIdMap);
			} else {
				SchemaUtils.toLocalJsonPath(schemaPath, line.hasOption("multiple"), schema, attrIdMap);
			}
			
		} else if (line.hasOption("gensql")) try {
			LogicSchema schema = SchemaUtils.fromLocalJsonPath(line.getOptionValue("from"), GenLogicSchema.class);
			String schemaName = line.getOptionValue("dbschema", DEFAULT_DB_SCHEMA);
			List<String> ddl = SchemaUtils.genCreateSqlByLogicSchema(schema, schemaName, DBType.VERTICA);
			if (ddl != null && ddl.size() > 0) {
				writer = new BufferedWriter(new FileWriter(line.getOptionValue("gensql")));
				for (String expr: ddl) {
					writer.write(expr);
					writer.write(';');
					writer.newLine();
				}
			}
		} finally {
			if (writer != null)
				writer.close();
			
		} else if (line.hasOption("genfiletablemapping")) try {
			LogicSchema schema = SchemaUtils.fromLocalJsonPath(line.getOptionValue("from"), GenLogicSchema.class);
			
			if (schema.getTableIdNameMap().size() > 0) {
				writer = new BufferedWriter(new FileWriter(line.getOptionValue("genfiletablemapping")));
				for (Map.Entry<String, String> e : schema.getTableIdNameMap().entrySet()) {
					writer.write("filename="+e.getKey());
					writer.newLine();
					writer.write("tablename="+e.getValue());
					writer.newLine();
				}
				writer.close();
			}
			
		} finally {
			if (writer != null)
				writer.close();
		}
	}

	private static LogicSchema createFinalLogicSchema(LogicSchema schema) {
		LogicSchema finalSchema = new LogicSchema();
		if (schema != null) {
			finalSchema.setAttrIdNameMap(schema.getAttrIdNameMap());
			finalSchema.setAttrNameMap(schema.getAttrNameMap());
			finalSchema.setAttrTypeMap(schema.getAttrTypeMap());
			finalSchema.setIndex(schema.isIndex());
			finalSchema.setTableIdNameMap(schema.getTableIdNameMap());
		}
		return schema;
	}
}
