package com.alibaba.alink.common.exceptions;

public class XGBoostPluginNotExistsException extends RuntimeException {

	public XGBoostPluginNotExistsException(String message) {
		super(message);
	}

	public XGBoostPluginNotExistsException(String message, Throwable cause) {
		super(message, cause);
	}

	public XGBoostPluginNotExistsException(Throwable cause) {
		super(cause);
	}
}
