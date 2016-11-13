package dv.db.dao;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import dv.db.entity.AccountEntity;


public interface CommonDao extends BaseRepository<AccountEntity, Serializable> {
	public List<Map<String, Object>> getNativeMapBySQL(String sql);
}
