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

@Table(name = "ALINK_NODE_PARAM_INFO", indexes = {
	@Index(name = "idx_nodeparam_id_unq", columnList = "id", unique = true),
	@Index(name = "idx_nodeparam_experimentid", columnList = "experimentId"),
	@Index(name = "idx_nodeparam_nodeid_pkey_unq", columnList = "nodeId, pkey", unique = true)
})
@Entity
public class NodeParam {

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
	 * Node ID
	 */
	@Column(nullable = false)
	Long nodeId;

	/**
	 * parameter key
	 */
	@Column(name="pkey", nullable = false)
	String key;

	/**
	 * parameter value, after jsonized
	 */
	@Column(length = 256 * 1024, nullable = false)
	String value;

	public Long getId() {
		return id;
	}

	public NodeParam setId(Long id) {
		this.id = id;
		return this;
	}

	public Timestamp getGmtCreate() {
		return gmtCreate;
	}

	public NodeParam setGmtCreate(Timestamp gmtCreate) {
		this.gmtCreate = gmtCreate;
		return this;
	}

	public Timestamp getGmtModified() {
		return gmtModified;
	}

	public NodeParam setGmtModified(Timestamp gmtModified) {
		this.gmtModified = gmtModified;
		return this;
	}

	public Long getExperimentId() {
		return experimentId;
	}

	public NodeParam setExperimentId(Long experimentId) {
		this.experimentId = experimentId;
		return this;
	}

	public Long getNodeId() {
		return nodeId;
	}

	public NodeParam setNodeId(Long nodeId) {
		this.nodeId = nodeId;
		return this;
	}

	public String getKey() {
		return key;
	}

	public NodeParam setKey(String key) {
		this.key = key;
		return this;
	}

	public String getValue() {
		return value;
	}

	public NodeParam setValue(String value) {
		this.value = value;
		return this;
	}
}
