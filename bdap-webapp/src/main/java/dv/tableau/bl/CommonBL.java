package dv.tableau.bl;

import java.util.List;

import dv.db.entity.AccountEntity;

public interface CommonBL {
	public boolean validateLogin(AccountEntity account);
	public List getAccountDetail(AccountEntity account);
	public List getAccountPermissions(AccountEntity account);
}
