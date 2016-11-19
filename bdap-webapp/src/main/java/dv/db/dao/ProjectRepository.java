package dv.db.dao;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import dv.db.entity.ProjectEntity;

public interface ProjectRepository extends JpaRepository<ProjectEntity, String> {
//    Optional<ProjectEntity> findById(int id);
}
