package dv.db.dao;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import dv.db.entity.TableauAccountEntity;

public interface TableauAccountRepository extends JpaRepository<TableauAccountEntity, String> {
//    Optional<TableauAccountEntity> findById(int id);
}
