package com.alibaba.alink.server.repository;

import com.alibaba.alink.server.domain.Node;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NodeRepository extends JpaRepository <Node, Long> {

	List <Node> findByExperimentId(Long experimentId);
}
