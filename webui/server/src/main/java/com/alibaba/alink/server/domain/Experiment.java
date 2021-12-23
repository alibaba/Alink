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

import static javax.persistence.GenerationType.IDENTITY;

@Entity
@Table(name = "alink_experiment_info", indexes = {
	@Index(name = "idx_experiment_id_unq", columnList = "id", unique = true)
})
public class Experiment {

	/**
	 * ID
	 */
	@Id
	@GeneratedValue(strategy = IDENTITY)
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
	 * Experiment configuration, in JSON format
	 */
	String config;
	/**
	 * Name
	 */
	@Column(length = 255, nullable = false)
	private String name;

	public Long getId() {
		return id;
	}

	public Experiment setId(Long id) {
		this.id = id;
		return this;
	}

	public Timestamp getGmtCreate() {
		return gmtCreate;
	}

	public Experiment setGmtCreate(Timestamp gmtCreate) {
		this.gmtCreate = gmtCreate;
		return this;
	}

	public Timestamp getGmtModified() {
		return gmtModified;
	}

	public Experiment setGmtModified(Timestamp gmtModified) {
		this.gmtModified = gmtModified;
		return this;
	}

	public String getConfig() {
		return config;
	}

	public Experiment setConfig(String config) {
		this.config = config;
		return this;
	}

	public String getName() {
		return name;
	}

	public Experiment setName(String name) {
		this.name = name;
		return this;
	}
}
