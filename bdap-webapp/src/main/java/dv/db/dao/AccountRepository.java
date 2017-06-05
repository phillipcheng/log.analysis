package dv.db.dao;

import java.util.Map;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.jpa.repository.JpaRepository;

import dv.db.entity.AccountEntity;

public interface AccountRepository extends JpaRepository<AccountEntity, String> {
//	Optional<AccountEntity> findByUserId(String userId);
//	AccountEntity save(AccountEntity entity);
//	AccountEntity findOne(String userName);
//	void delete(String userName);
//	void delete(AccountEntity entity);
	public Page<Object[]> getNativeByCondition(Map map);
	
	
}
