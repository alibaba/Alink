package com.alibaba.alink.server.repository;

import com.alibaba.alink.server.domain.Experiment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ExperimentRepository extends JpaRepository <Experiment, Long> {
}
