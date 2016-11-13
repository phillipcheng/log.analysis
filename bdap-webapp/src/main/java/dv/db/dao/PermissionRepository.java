package dv.db.dao;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import dv.db.entity.PermissionEntity;

public interface PermissionRepository extends JpaRepository<PermissionEntity, String> {
//    Optional<PermissionEntity> findById(int id);
}
