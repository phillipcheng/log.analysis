package etl.tools;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import etl.engine.LogicSchema;
import etl.util.JsonUtil;

public class MigrateLogicSchema {
	public static final Logger logger = LogManager.getLogger(MigrateLogicSchema.class);
	public static void addTree(Path directory, final Collection<Path> all)
	        throws IOException {
	    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
	        @Override
	        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
	                throws IOException {
	        	String fileName=file.getFileName().toString().toLowerCase();
	        	if (fileName.contains("schema") && !fileName.endsWith(".properties") 
	        			&& !fileName.endsWith(".new")
	        			&& attrs.isRegularFile()){
	        		all.add(file);
	        	}
	            return FileVisitResult.CONTINUE;
	        }
	    });
	}
	
	@Test
	public void testMigrate() throws IOException{
		Path dir=java.nio.file.FileSystems.getDefault().getPath("src/test/resources");
		List<Path> allSchemaFiles = new ArrayList<Path>();
		addTree(dir, allSchemaFiles);
		for (Path schemaFile:allSchemaFiles){
			String infile = schemaFile.toString();
			logger.info(String.format("input:%s", infile));
			JsonUtil.migrateJson(infile, infile, LogicSchema.class);
		}
	}

}
