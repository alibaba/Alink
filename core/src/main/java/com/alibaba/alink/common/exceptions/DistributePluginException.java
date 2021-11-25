package com.alibaba.alink.common.exceptions;

import java.io.IOException;

public class DistributePluginException extends IOException {

	public DistributePluginException(String message) {
		super(message);
	}

	public DistributePluginException(String message, Throwable cause) {
		super(message, cause);
	}
}
