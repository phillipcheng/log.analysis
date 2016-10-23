package dv.entity;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class AccountEntity {

	@Id
	private String userName;
	private String password;
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
}
