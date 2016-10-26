package dv.db.dao;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import dv.db.entity.GroupEntity;

public interface GroupRepository extends JpaRepository<GroupEntity, String> {
    Optional<GroupEntity> findById(int id);
}
