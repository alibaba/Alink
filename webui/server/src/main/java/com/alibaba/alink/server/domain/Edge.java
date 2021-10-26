package com.alibaba.alink.server.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.sql.Timestamp;

@Entity
@Table(name = "alink_edge_info", indexes = {
	@Index(name = "idx_edge_id_unq", columnList = "id", unique = true),
	@Index(name = "idx_edge_experiment_id", columnList = "experimentId")
})
public class Edge {

	/**
	 * ID
	 */
	@Id
	@GeneratedValue
	Long id;

	/**
	 * Creation timestamp
	 */
	@CreationTimestamp
	@Column(nullable = false, updatable = false)
	Timestamp gmtCreate;

	/**
	 * Update timestamp
	 */
	@UpdateTimestamp
	@Column(nullable = false)
	Timestamp gmtModified;

	/**
	 * Experiment ID
	 */
	@Column(nullable = false)
	Long experimentId;

	/**
	 * Source node ID
	 */
	@Column(nullable = false)
	Long srcNodeId;

	/**
	 * Source node port
	 */
	@Column(nullable = false)
	Short srcNodePort;

	/**
	 * Destination node ID
	 */
	@Column(nullable = false)
	Long dstNodeId;

	/**
	 * Destination node port
	 */
	@Column(nullable = false)
	Short dstNodePort;

	public Long getId() {
		return id;
	}

	public Edge setId(Long id) {
		this.id = id;
		return this;
	}

	public Timestamp getGmtCreate() {
		return gmtCreate;
	}

	public Edge setGmtCreate(Timestamp gmtCreate) {
		this.gmtCreate = gmtCreate;
		return this;
	}

	public Timestamp getGmtModified() {
		return gmtModified;
	}

	public Edge setGmtModified(Timestamp gmtModified) {
		this.gmtModified = gmtModified;
		return this;
	}

	public Long getExperimentId() {
		return experimentId;
	}

	public Edge setExperimentId(Long experimentId) {
		this.experimentId = experimentId;
		return this;
	}

	public Long getSrcNodeId() {
		return srcNodeId;
	}

	public Edge setSrcNodeId(Long srcNodeId) {
		this.srcNodeId = srcNodeId;
		return this;
	}

	public Short getSrcNodePort() {
		return srcNodePort;
	}

	public Edge setSrcNodePort(Short srcNodePort) {
		this.srcNodePort = srcNodePort;
		return this;
	}

	public Long getDstNodeId() {
		return dstNodeId;
	}

	public Edge setDstNodeId(Long dstNodeId) {
		this.dstNodeId = dstNodeId;
		return this;
	}

	public Short getDstNodePort() {
		return dstNodePort;
	}

	public Edge setDstNodePort(Short dstNodePort) {
		this.dstNodePort = dstNodePort;
		return this;
	}
}
