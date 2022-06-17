package com.alibaba.alink.common.exceptions;

public class AkFlinkExecutionErrorException extends ExceptionWithErrorCode {
	public AkFlinkExecutionErrorException(String message) {
		super(ErrorCode.FLINK_EXECUTION_ERROR, message);
	}
}
