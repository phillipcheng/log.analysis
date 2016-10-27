package dv.db.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="t_account")
public class AccountEntity {

	@Id
	@GeneratedValue
	private int userId;
	@Column(name="name",length=50)  
	private String userName;
	private String password;
	private int groupid;
	private int tableauUserId;
	private String email;
	
	public AccountEntity(){
	}
	
	public AccountEntity(String userName, String password){
		this.userName = userName;
		this.password = password;
	}
	
	public String getUserName() {
		return userName;
	}
	public String getPassword() {
		return password;
	}
	public String getEmail() {
		return email;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public void setEmail(String email) {
		this.email = email;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserid(int userId) {
		this.userId = userId;
	}

	public int getGroupid() {
		return groupid;
	}

	public void setGroupid(int groupid) {
		this.groupid = groupid;
	}

	public int getTableauUserId() {
		return tableauUserId;
	}

	public void setTableauUserId(int tableauUserId) {
		this.tableauUserId = tableauUserId;
	}
}
