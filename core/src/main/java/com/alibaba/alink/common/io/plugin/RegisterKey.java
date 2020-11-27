package com.alibaba.alink.common.io.plugin;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

public class RegisterKey implements Serializable {
	private static final long serialVersionUID = 7679702042658791364L;
	private final String name;
	private final String version;

	public RegisterKey(String name, String version) {
		this.name = name;
		this.version = version;
	}

	public String getName() {
		return name;
	}

	public String getVersion() {
		return version;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 31)
			.append(name)
			.append(version)
			.toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof RegisterKey)) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		RegisterKey registerKey = (RegisterKey) obj;

		return new EqualsBuilder()
			.append(name, registerKey.name)
			.append(version, registerKey.version)
			.isEquals();
	}
}
