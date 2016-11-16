package bdap.util;

public class CmdDef{
	private String className;
	private boolean hasReduce;
	
	public CmdDef(String className, boolean hasReduce){
		this.setClassName(className);
		this.setHasReduce(hasReduce);
	}
	
	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public boolean isHasReduce() {
		return hasReduce;
	}

	public void setHasReduce(boolean hasReduce) {
		this.hasReduce = hasReduce;
	}
}