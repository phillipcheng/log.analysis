package dv.db.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="t_flow")
public class FlowEntity {
	public FlowEntity(){
	}
	
	public FlowEntity(String name, String owner, String jsonContent){
		this.name = name;
		this.owner = owner;
		this.jsonContent= jsonContent;
	}
	@Id
	private String name;
	
	private String owner;
	@Column(length=100000)  
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
