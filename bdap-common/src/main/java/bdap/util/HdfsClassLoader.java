package bdap.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class HdfsClassLoader extends ClassLoader {
    /**
     * A configuration option to indicate whether or not to skip the standard {@link ClassLoader} hierarchical
     * search and look at the configured jar first.  Defaults to {@code false} so that the standard search is performed,
     * but it may be helpful to turn to {@code true} for debugging.
     */
    public static final String ATTEMPT_LOCAL_LOAD_FIRST = "hdfs.classloader.attemptLocalFirst";

	public static final Logger LOG = LogManager.getLogger(HdfsClassLoader.class);

    private final FileSystem fs;
    private final String libPath;

    /**
     * @param configuration The Hadoop configuration to use to read from HDFS
     * @param jar A path to a jar file containing classes to load
     */
    public HdfsClassLoader(FileSystem fs, String libPath) {
        super(HdfsClassLoader.class.getClassLoader());
        this.fs = fs;
        this.libPath = libPath;
    }
    
    /**
     * Search for the class in the configured jar file stored in HDFS.
     *
     * {@inheritDoc}
     */
    public Class<?> findClass(String className) throws ClassNotFoundException {
        String classPath = convertNameToPath(className);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Searching for class %s (%s) in path %s", className, classPath, this.libPath));
        }
        JarInputStream jarIn = null;
        List<String> files = HdfsUtil.listDfsFilePath(fs, libPath, true);
        Path jar;
        for (String f: files) {
        	if (f != null && f.endsWith(".jar")) {
		        try {
		        	jar = new Path(f);
		            jarIn = new JarInputStream(fs.open(jar));
		            JarEntry currentEntry = null;
		            while ((currentEntry = jarIn.getNextJarEntry()) != null) {
		                if (LOG.isTraceEnabled()) {
		                    LOG.trace(String.format("Comparing %s to entry %s", classPath, currentEntry.getName()));
		                }
		                if (currentEntry.getName().equals(classPath)) {
		                    byte[] classBytes = readEntry(jarIn);
		                    return defineClass(className, classBytes, 0, classBytes.length);
		                }
		            }
		        }
		        catch (IOException ioe) {
		        	LOG.error(ioe.getMessage(), ioe);
		        }
		        finally {
		            closeQuietly(jarIn);
		            // While you would think it would be prudent to close the filesystem that you opened,
		            // it turns out that this filesystem is shared with HBase, so when you close this one,
		            // it becomes closed for HBase, too.  Therefore, there is no call to closeQuietly(fs);
		        }
		    }
        }
        throw new ClassNotFoundException("Unable to find " + className + " in path " + this.libPath);
    }
    
	public List<Class<?>> findClasses(String[] searchPackages, Class<?> baseClass) {
		List<Class<?>> classes = new ArrayList<Class<?>>();
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Searching for classes in path %s", this.libPath));
        }
        JarInputStream jarIn = null;
        List<String> files = HdfsUtil.listDfsFilePath(fs, libPath, true);
        Path jar;
        String clsName;
        Class<?> cls;
        for (String f: files) {
        	if (f != null && f.endsWith(".jar")) {
		        try {
		        	jar = new Path(f);
		            jarIn = new JarInputStream(fs.open(jar));
		            JarEntry currentEntry = null;
		            while ((currentEntry = jarIn.getNextJarEntry()) != null) {
		                if (LOG.isTraceEnabled()) {
		                    LOG.trace(String.format("Comparing entry %s", currentEntry.getName()));
		                }
		                if (!currentEntry.isDirectory() && compareWith(currentEntry.getName(), searchPackages)) {
		                	clsName = className(currentEntry.getName());
		                	cls = findLoadedClass(clsName);
		                	if (cls == null) {
			                    byte[] classBytes = readEntry(jarIn);
			                    cls = defineClass(clsName, classBytes, 0, classBytes.length);
			                    if (baseClass != null && cls != null && baseClass.isAssignableFrom(cls))
			                    	classes.add(cls);
		                	}
		                }
		            }
		        }
		        catch (IOException ioe) {
		        	LOG.error(ioe.getMessage(), ioe);
		        }
		        finally {
		            closeQuietly(jarIn);
		            // While you would think it would be prudent to close the filesystem that you opened,
		            // it turns out that this filesystem is shared with HBase, so when you close this one,
		            // it becomes closed for HBase, too.  Therefore, there is no call to closeQuietly(fs);
		        }
		    }
        }
        return classes;
	}

    private boolean compareWith(String classPath, String[] searchPackages) {
    	if (classPath != null && classPath.endsWith(".class") && searchPackages != null) {
    		String clsName = className(classPath);
    		for (String pkg: searchPackages)
    			if (clsName.startsWith(pkg))
    				return true;
    	}
		return false;
	}

	private String className(String classPath) {
		return classPath.substring(0, classPath.length() - 6).replace('/', '.');
	}

	/**
     * Converts a binary class name to the path that it would show up as in a jar file.
     * For example, java.lang.String would show up as a jar entry at java/lang/String.class
     */
    private String convertNameToPath(String className) {
        String classPath = className.replace('.', '/');
        classPath += ".class";
        return classPath;
    }

    /**
     * Read an entry out of the jar file
     */
    private byte[] readEntry(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = in.read(buffer, 0, buffer.length)) > 0) {
            out.write(buffer, 0, read);
        }
        out.flush();
        out.close();
        return out.toByteArray();
    }

    /**
     * Close the {@link Closeable} without any exceptions
     */
    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (IOException ioe) {
                // the point of being quiet is to not propogate this
            }
        }
    }
}
