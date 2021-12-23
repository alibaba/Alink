package com.alibaba.alink.server.repository;

import com.alibaba.alink.server.domain.NodeParam;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NodeParamRepository extends JpaRepository<NodeParam, Long> {

	List <NodeParam> findByExperimentId(Long experimentId);

	List <NodeParam> findByExperimentIdAndNodeId(Long experimentId, Long nodeId);

}
