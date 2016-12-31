package dv.db.dao;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import dv.db.entity.FlowInstanceEntity;

public interface FlowInstanceRepository extends JpaRepository<FlowInstanceEntity, String>{
	List<FlowInstanceEntity> findByFlowName(String flowName);
	

}
