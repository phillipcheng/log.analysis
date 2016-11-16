package bdap.tools.pushagent;

import java.io.File;
import java.util.Comparator;

public class FileAgeComparator implements Comparator<File> {

	public int compare(File f1, File f2) {
		if (f1.lastModified() != f2.lastModified())
			return Long.compare(f1.lastModified(), f2.lastModified());
		else
			return f1.compareTo(f2);
	}

}
