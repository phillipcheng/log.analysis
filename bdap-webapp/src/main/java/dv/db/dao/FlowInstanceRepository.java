package dv.db.dao;

import org.springframework.data.jpa.repository.JpaRepository;
import dv.db.entity.FlowInstanceEntity;

public interface FlowInstanceRepository extends JpaRepository<FlowInstanceEntity, String>{

	

}
