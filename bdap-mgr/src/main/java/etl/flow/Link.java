package etl.flow;

import java.util.Objects;

public class Link {

	private String fromNodeName;
	private String toNodeName;
	private LinkType linkType = LinkType.success;

	public String toString(){
		return String.format("%s_%s_%d_%d_%s", this.fromNodeName, this.toNodeName);
	}

	public Link(){
	}
	
	public Link(String fromNN, String toNN){
		this.fromNodeName = fromNN;
		this.toNodeName = toNN;
	}
	
	public Link(String fromNN, String toNN, LinkType linkType){
		this(fromNN, toNN);
		this.linkType = linkType;
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof Link)){
			return false;
		}
		Link that = (Link) obj;
		if (!Objects.equals(fromNodeName, that.getFromNodeName())){
			return false;
		}
		if (!Objects.equals(toNodeName, that.getToNodeName())){
			return false;
		}
		if (!Objects.equals(linkType, that.getLinkType())){
			return false;
		}
		return true;
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

	public LinkType getLinkType() {
		return linkType;
	}

	public void setLinkType(LinkType linkType) {
		this.linkType = linkType;
	}
}
