package dv.db.dao;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import dv.db.entity.ProjectEntity;

@Transactional(readOnly = true)
public interface ProjectRepository extends JpaRepository<ProjectEntity, Integer> {
	@Query("select t from ProjectEntity t where t.projectName = ?1")
    ProjectEntity findByName(String name);
	
	@Modifying
	@Transactional
    @Query("delete from ProjectEntity t where t.projectName = ?1")
	void deleteByName(String name);
}
