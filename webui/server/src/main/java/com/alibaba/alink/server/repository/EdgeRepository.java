package com.alibaba.alink.server.repository;

import com.alibaba.alink.server.domain.Edge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface EdgeRepository extends JpaRepository <Edge, Long> {
	List <Edge> findByExperimentId(Long experimentId);
}
