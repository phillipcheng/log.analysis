package dv.db.dao;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import dv.db.entity.FlowEntity;

public interface FlowRepository extends JpaRepository<FlowEntity, String> {
//    Optional<FlowEntity> findByOwner(String owner);
}
