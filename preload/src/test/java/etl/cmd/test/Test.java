package etl.cmd.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test {
	public static void main(String[] args) {
		List<String> inputFiles = new ArrayList<String> (Arrays.asList("a","b","c"));
		System.out.println(inputFiles);
		System.out.println(inputFiles.contains("a"));
	}

}
