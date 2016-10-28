package dv.db.dao;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import dv.db.entity.AccountEntity;

public interface AccountRepository extends JpaRepository<AccountEntity, String> {
	Optional<AccountEntity> findByUserId(String userId);
}
