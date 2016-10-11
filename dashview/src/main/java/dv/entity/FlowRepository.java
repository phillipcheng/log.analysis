package dv.entity;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FlowRepository extends JpaRepository<FlowEntity, String> {
    Optional<FlowEntity> findByOwner(String owner);
}
