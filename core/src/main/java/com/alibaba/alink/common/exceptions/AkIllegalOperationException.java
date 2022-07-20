package com.alibaba.alink.common.exceptions;

public class AkIllegalOperationException extends ExceptionWithErrorCode {
	public AkIllegalOperationException(String message) {
		super(ErrorCode.ILLEGAL_OPERATION, message);
	}

	public AkIllegalOperationException(String message, Throwable cause) {
		super(ErrorCode.ILLEGAL_OPERATION, message, cause);
	}
}
