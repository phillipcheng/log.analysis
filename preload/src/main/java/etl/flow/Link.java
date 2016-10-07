package etl.flow;

public class Link {
	
	private String name;
	private String fromNodeName;
	private String toNodeName;
	private String dataName = null;
	private int fromActionDataOutletIdx = 0;
	private int toActionDataInletIdx = 0;
	private LinkType linkType = LinkType.success;

	public String toString(){
		return String.format("Link:%s", name);
	}
	
	private String genName(){
		return String.format("%s_%s_%d_%d", this.fromNodeName, this.toNodeName, this.fromActionDataOutletIdx, this.toActionDataInletIdx);
	}
	
	public Link(String fromNN, String toNN){
		this.fromNodeName = fromNN;
		this.toNodeName = toNN;
		this.setName(genName());
	}
	
	public Link(String fromNN, String toNN, LinkType linkType, String dataN, int fromIdx, int toIdx){
		this(fromNN, toNN);
		this.linkType = linkType;
		this.dataName = dataN;
		this.fromActionDataOutletIdx = fromIdx;
		this.toActionDataInletIdx = toIdx;
	}
	
	@Override
	public boolean equals(Object obj){
		if (obj instanceof Link){
			Link that = (Link) obj;
			if (name.equals(that.getName())){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}

	public String getFromNodeName() {
		return fromNodeName;
	}

	public void setFromNodeName(String fromNodeName) {
		this.fromNodeName = fromNodeName;
	}

	public String getToNodeName() {
		return toNodeName;
	}

	public void setToNodeName(String toNodeName) {
		this.toNodeName = toNodeName;
	}

	public String getDataName() {
		return dataName;
	}

	public void setDataName(String dataName) {
		this.dataName = dataName;
	}

	public int getFromActionDataOutletIdx() {
		return fromActionDataOutletIdx;
	}

	public void setFromActionDataOutletIdx(int fromActionDataOutletIdx) {
		this.fromActionDataOutletIdx = fromActionDataOutletIdx;
	}

	public int getToActionDataInletIdx() {
		return toActionDataInletIdx;
	}

	public void setToActionDataInletIdx(int toActionDataInletIdx) {
		this.toActionDataInletIdx = toActionDataInletIdx;
	}

	public LinkType getLinkType() {
		return linkType;
	}

	public void setLinkType(LinkType linkType) {
		this.linkType = linkType;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
