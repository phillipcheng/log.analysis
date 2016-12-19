package bdap.tools.pushagent;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.WildcardFileFilter;

public class PathFilter extends WildcardFileFilter {
	private static final long serialVersionUID = 1L;

	// Ideally this would just use the same variable in the parent, but it's
    // not accessible
    private IOCase caseSensitivity;

    // Same here
    private String wildcard;
    
    private Path pathBase;

    /**
     * Construct a new <code>IOFileFilter</code> specifying whether the
     * wildcard criteria.
     * 
     * @param wildcardmatcher the wildcard string to match against
     * @param ignoreCase whether to ignore the case
     */
    public PathFilter(String wildcardmatcher, boolean ignoreCase) {
    	this(File.separator, wildcardmatcher, ignoreCase);
    }


    public PathFilter(String basePath, String wildcardmatcher, boolean ignoreCase) {
        super(wildcardmatcher, ignoreCase ? IOCase.INSENSITIVE : IOCase.SENSITIVE);
        this.caseSensitivity = ignoreCase ? IOCase.INSENSITIVE : IOCase.SENSITIVE;
        this.wildcard = wildcardmatcher;
        this.pathBase = Paths.get(basePath);
	}


	/**
     * Checks to see if the filename matches the wildcard.
     *
     * @param dir  the file directory
     * @param name  the full filename
     * @return true if the full filename matches the wildcard
     */
    public boolean accept(File dir, String name) {
    	File f = new File(dir, name);
        Path pathAbsolute = f.toPath();
        Path pathRelative = pathBase.relativize(pathAbsolute);
        if (FilenameUtils.wildcardMatch(pathRelative.toString(), wildcard, caseSensitivity)) {
            return true;
        }
        return false;
    }

    /**
     * Checks to see if the full filename matches the wildcard.
     *
     * @param file  the file to check
     * @return true if the full filename matches the wildcard
     */
    public boolean accept(File file) {
        Path pathAbsolute = file.toPath();
        Path pathRelative = pathBase.relativize(pathAbsolute);
        if (FilenameUtils.wildcardMatch(pathRelative.toString(), wildcard, caseSensitivity)) {
            return true;
        }
        return false;
    }

}
