package dv.entity.flow;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class FlowEntity {
	
	@Id
	private String name;
	
	private String owner;
	
	private String jsonContent;

	public String getName() {
		return name;
	}

	public String getJsonContent() {
		return jsonContent;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setJsonContent(String jsonContent) {
		this.jsonContent = jsonContent;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

}
