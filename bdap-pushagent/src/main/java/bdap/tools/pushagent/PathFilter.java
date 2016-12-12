package bdap.tools.pushagent;

import java.io.File;

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

    /**
     * Construct a new <code>IOFileFilter</code> specifying whether the
     * wildcard criteria.
     * 
     * @param wildcardmatcher the wildcard string to match against
     * @param ignoreCase whether to ignore the case
     */
    public PathFilter(String wildcardmatcher, boolean ignoreCase) {
        super(wildcardmatcher, ignoreCase ? IOCase.INSENSITIVE : IOCase.SENSITIVE);
        this.caseSensitivity = ignoreCase ? IOCase.INSENSITIVE : IOCase.SENSITIVE;
        this.wildcard = wildcardmatcher;
    }


    /**
     * Checks to see if the filename matches the wildcard.
     *
     * @param dir  the file directory
     * @param name  the full filename
     * @return true if the full filename matches the wildcard
     */
    public boolean accept(File dir, String name) {
        String path = new File(dir, name).toString();
        if (FilenameUtils.wildcardMatch(path, wildcard, caseSensitivity)) {
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
        String name = file.getPath();
        if (FilenameUtils.wildcardMatch(name, wildcard, caseSensitivity)) {
            return true;
        }
        return false;
    }

}
