package com.alibaba.alink.common.exceptions;

import java.io.IOException;

public class PluginNotExistException extends IOException {

	public PluginNotExistException(String message) {
		super(message);
	}

	public PluginNotExistException(String message, Throwable cause) {
		super(message, cause);
	}
}
