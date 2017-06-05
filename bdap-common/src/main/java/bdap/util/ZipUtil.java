package bdap.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ZipUtil {
	public static final Logger logger = LogManager.getLogger(ZipUtil.class);
	
	public static void makeJar(String output, String inputDir) throws IOException {
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		JarOutputStream target = new JarOutputStream(new FileOutputStream(output), manifest);
		add(new File(inputDir), target, new File(inputDir));
		target.close();
	}

	
	private static void add(File source, JarOutputStream target, File rootDir) throws IOException {
		
		BufferedInputStream in = null;
		try {
			if (source.isDirectory()) {
				String name = source.getPath().replace("\\", "/");
				String root = rootDir.getPath().replace("\\", "/");
				if (!name.isEmpty()){
					if (!name.endsWith("/"))
						name += "/";
					String entryName = name.substring(root.length()+1);
					if (entryName.length()>0){
						JarEntry entry = new JarEntry(entryName);
						entry.setTime(source.lastModified());
						target.putNextEntry(entry);
						logger.info(String.format("add entry:%s", entry));
						target.closeEntry();
					}
				}
				for (File nestedFile: source.listFiles())
					add(nestedFile, target, rootDir);
				return;
			}
			String name = source.getPath().replace("\\", "/");
			String root = rootDir.getPath().replace("\\", "/");
			String entryName = name.substring(root.length()+1);
			JarEntry entry = new JarEntry(entryName);
			logger.info(String.format("add entry:%s", entry));
			entry.setTime(source.lastModified());
			target.putNextEntry(entry);
			in = new BufferedInputStream(new FileInputStream(source));

			byte[] buffer = new byte[1024];
			while (true) {
				int count = in.read(buffer);
				if (count == -1)
					break;
				target.write(buffer, 0, count);
			}
			target.closeEntry();
		}finally{
			if (in != null)
				in.close();
		}
	}
}
