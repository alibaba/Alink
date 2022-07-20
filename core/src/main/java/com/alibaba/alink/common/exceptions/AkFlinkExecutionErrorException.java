package com.alibaba.alink.common.exceptions;

public class AkFlinkExecutionErrorException extends ExceptionWithErrorCode {
	public AkFlinkExecutionErrorException(String message) {
		super(ErrorCode.FLINK_EXECUTION_ERROR, message);
	}

	public AkFlinkExecutionErrorException(String message, Throwable cause) {
		super(ErrorCode.FLINK_EXECUTION_ERROR, message, cause);
	}
}
