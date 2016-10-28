package dv.tableau.bl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import dv.db.dao.AccountRepository;
import dv.db.dao.AccountRepositoryImpl;
import dv.db.dao.CommonDaoImpl;
import dv.db.entity.AccountEntity;

@Service("CommonBL")
public class CommonBLImpl implements CommonBL {
	
	@Autowired
	AccountRepository accountRep;
	@Autowired
	AccountRepositoryImpl accountRepImp;
	@Autowired
	CommonDaoImpl commonDao;
	
	public boolean validateLogin(AccountEntity account) {
		String userName = account.getUserName();
		String password = account.getPassword();
		String sql = "select count(1) as count_ from T_ACCOUNT  where name = '%s' and password = '%s'";
		sql = String.format(sql, userName, password);
		List list = commonDao.getNativeMapBySQL(sql);
		Map map = (Map) list.get(0);
		Long count = (Long)map.get("count_");
		if(count > 0) {
			return true;
		}
		return false;
	}
	
	public List getAccountDetail(AccountEntity account) {
		String userName = account.getUserName();
		String password = account.getPassword();
		String sql = "select a.userid, a.name username, a.password, b.username  tabuser, b.password  tabpassword from T_ACCOUNT a left join T_TABLEAU_ACCOUNT b on a.tableauuserid=b.id  where a.name = '%s' and a.password = '%s'";
		sql = String.format(sql, userName, password);
		List list = commonDao.getNativeMapBySQL(sql);
		if(list != null && list.size() > 0) {
			return list;
		}
		return null;
	}
	
	public List getAccountPermissions(AccountEntity account) {
		String userName = account.getUserName();
		String password = account.getPassword();
		StringBuffer buffer = new StringBuffer();
		buffer.append(" select pro.id, pro.projectName, pro.content, pro.type, per.action from t_permission per left join t_project pro on per.projectid=pro.id");
		buffer.append(" where id in");
		buffer.append(" (select permissionIds from t_group where id in ");
		buffer.append(" (select groupids from t_account where name='%s' and password='%s'))");
		String sql = buffer.toString();
		sql = String.format(sql, userName, password);
		List list = commonDao.getNativeMapBySQL(sql);
		if(list != null && list.size() > 0) {
			return list;
		}
		return null;
	}
	
	
}
