package etl.preload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

public class FileMerger {
	private static final Logger logger = Logger.getLogger(FileMerger.class);
	
	//return the output line which is merged from leftLine and rightLine
	private String processLine(FileMergerConf fms, String leftLine, String rightLine){
		String[] leftArray = leftLine.split(",");
		String[] rightArray = rightLine.split(",");
		String output=null;
		String leftKey=leftArray[fms.getLeftKeyCol()-1];
		String rightKey = rightArray[fms.getRightKeyCol()-1];
		if (leftKey.equals(rightKey)){
			//key value matches
			output=leftLine;
			for (int i=0; i<fms.getRightValCols().length;i++){
				int idx = fms.getRightValCols()[i];
				output += rightArray[idx-1] + ",";
			}
		}
		return output;
	}
	
	public void processFile(String property, String leftFile, String rightFile, String outputFile){
		FileMergerConf fmc = new FileMergerConf(property);
		BufferedReader brleft = null;
		BufferedReader brright = null;
		PrintWriter fw = null;
		try {
			fw = new PrintWriter(new FileWriter(outputFile));
			brleft = new BufferedReader(new FileReader(leftFile));
			brright = new BufferedReader(new FileReader(rightFile));
			String leftLine;
			String rightLine;
			String outLine;
			if (fmc.isLeftHasHeader()){
				brleft.readLine();
			}
			if (fmc.isRightHasHeader()){
				brright.readLine();
			}
			while ((leftLine = brleft.readLine()) != null) {
				rightLine = brright.readLine();
				if (rightLine!=null){
					outLine = processLine(fmc, leftLine, rightLine);
					if (outLine==null){
						logger.error(String.format("Can not merge %s with %s", leftLine, rightLine));
					}else{
						fw.println(outLine);
					}
				}
			}
		}catch(Exception e){
			logger.error("", e);
		}finally{
			if (brleft!= null){
				try{
					brleft.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
			if (brright!= null){
				try{
					brright.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
			if (fw!= null){
				try{
					fw.close();
				}catch(Exception e){
					logger.error("", e);
				}
			}
		}
	}
	
	public static void main(String[] args){
		FileMerger fm = new FileMerger();
		if (args.length!=4){
			System.out.print("usage: FileMerger pde.cvs.fix.merger.properties leftFile rightFile outputFile");
		}else{
			String propertyFile = args[0];
			String leftFile = args[1];
			String rightFile = args[2];
			String outputFile = args[3];
			fm.processFile(propertyFile, leftFile, rightFile, outputFile);
		}
	}
}
