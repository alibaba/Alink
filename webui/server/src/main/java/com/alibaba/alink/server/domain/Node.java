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
@Table(name = "alink_node_info", indexes = {
	@Index(name = "idx_node_id_unq", columnList = "id", unique = true),
	@Index(name = "idx_node_experiment_id", columnList = "experimentId")
})
public class Node {

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
	 * Node type
	 */
	@Column(nullable = false)
	NodeType type;

	/**
	 * Node display name
	 */
	@Column(nullable = false)
	String name;

	/**
	 * X coordinate of node position
	 */
	@Column(nullable = false)
	Double positionX;

	/**
	 * Y coordinate of node position
	 */
	@Column(nullable = false)
	Double positionY;

	/**
	 * Algorithm class name
	 */
	@Column(nullable = false)
	String className;

	public Long getId() {
		return id;
	}

	public Node setId(Long id) {
		this.id = id;
		return this;
	}

	public Timestamp getGmtCreate() {
		return gmtCreate;
	}

	public Node setGmtCreate(Timestamp gmtCreate) {
		this.gmtCreate = gmtCreate;
		return this;
	}

	public Timestamp getGmtModified() {
		return gmtModified;
	}

	public Node setGmtModified(Timestamp gmtModified) {
		this.gmtModified = gmtModified;
		return this;
	}

	public Long getExperimentId() {
		return experimentId;
	}

	public Node setExperimentId(Long experimentId) {
		this.experimentId = experimentId;
		return this;
	}

	public NodeType getType() {
		return type;
	}

	public Node setType(NodeType type) {
		this.type = type;
		return this;
	}

	public String getName() {
		return name;
	}

	public Node setName(String name) {
		this.name = name;
		return this;
	}

	public Double getPositionX() {
		return positionX;
	}

	public Node setPositionX(Double positionX) {
		this.positionX = positionX;
		return this;
	}

	public Double getPositionY() {
		return positionY;
	}

	public Node setPositionY(Double positionY) {
		this.positionY = positionY;
		return this;
	}

	public String getClassName() {
		return className;
	}

	public Node setClassName(String className) {
		this.className = className;
		return this;
	}
}
