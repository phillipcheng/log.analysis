package etl.preload;


import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class FileMergerConf {
	
	public static final Logger logger = Logger.getLogger(FileMergerConf.class);
	
	public static final String LEFT_KEY_COL="left.key.col";
	public static final String LEFT_HEADER="left.has.header";
	public static final String RIGHT_KEY_COL="right.key.col";
	public static final String RIGHT_HEADER="right.has.header";
	public static final String RIGHT_VAL_COLS="right.val.cols";
	
	private int leftKeyCol;
	private boolean leftHasHeader;
	private int rightKeyCol;
	private boolean rightHasHeader;
	private int[] rightValCols;
	
	public FileMergerConf(String fileName){
		PropertiesConfiguration pc = null;
		try{
			pc =new PropertiesConfiguration(fileName);
		}catch(Exception e){
			logger.error("", e);
		}
		
		//record overall configuration
		leftKeyCol = pc.getInt(LEFT_KEY_COL);
		setLeftHasHeader(pc.getBoolean(LEFT_HEADER));
		rightKeyCol = pc.getInt(RIGHT_KEY_COL);
		setRightHasHeader(pc.getBoolean(RIGHT_HEADER));
		String[] rightValStr = pc.getStringArray(RIGHT_VAL_COLS);
		rightValCols = new int[rightValStr.length];
		for (int i=0; i<rightValStr.length; i++){
			rightValCols[i] = Integer.parseInt(rightValStr[i]);
		}	
	}

	public int getLeftKeyCol() {
		return leftKeyCol;
	}

	public void setLeftKeyCol(int leftKeyCol) {
		this.leftKeyCol = leftKeyCol;
	}

	public int getRightKeyCol() {
		return rightKeyCol;
	}

	public void setRightKeyCol(int rightKeyCol) {
		this.rightKeyCol = rightKeyCol;
	}

	public int[] getRightValCols() {
		return rightValCols;
	}

	public void setRightValCols(int[] rightValCols) {
		this.rightValCols = rightValCols;
	}

	public boolean isLeftHasHeader() {
		return leftHasHeader;
	}

	public void setLeftHasHeader(boolean leftHasHeader) {
		this.leftHasHeader = leftHasHeader;
	}

	public boolean isRightHasHeader() {
		return rightHasHeader;
	}

	public void setRightHasHeader(boolean rightHasHeader) {
		this.rightHasHeader = rightHasHeader;
	}
}
