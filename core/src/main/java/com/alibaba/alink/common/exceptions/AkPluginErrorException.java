package com.alibaba.alink.common.exceptions;

public class AkPluginErrorException extends ExceptionWithErrorCode {
	public AkPluginErrorException(String message) {
		super(ErrorCode.PLUGIN_ERROR, message);
	}

	public AkPluginErrorException(String message, Throwable cause) {
		super(ErrorCode.PLUGIN_ERROR, message, cause);
	}
}
