package com.alibaba.alink.common.io.catalog.odps;

import java.io.Serializable;

public class OdpsConf implements Serializable {
	String accessId;
	String accessKey;
	String endpoint;
	String tunnelEndpoint;
	String project;

	//public OdpsConf(String accessId, String accessKey, String endpoint) {
	//	this(accessId, accessKey, endpoint, "", "");
	//}

	public OdpsConf(String accessId, String accessKey, String endpoint, String project) {
		this(accessId, accessKey, endpoint, project, "");
	}

	public OdpsConf(String accessId, String accessKey, String endpoint, String project, String tunnelEndpoint) {
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
		this.project = project;
		this.tunnelEndpoint = tunnelEndpoint;
	}

	public void setAccessId(String accessId) {
		this.accessId = accessId;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public void setTunnelEndpoint(String tunnelEndpoint) {
		this.tunnelEndpoint = tunnelEndpoint;
	}

	public String getTunnelEndpoint() {
		return this.tunnelEndpoint;
	}

	public String getAccessId() {
		return this.accessId;
	}

	public String getAccessKey() {
		return this.accessKey;
	}

	public String getEndpoint() {
		return this.endpoint;
	}

	public String getProject() {
		return this.project;
	}
}
