package com.alibaba.alink.common.exceptions;

public class AkIllegalStateException extends ExceptionWithErrorCode {
	public AkIllegalStateException(String message) {
		super(ErrorCode.ILLEGAL_STATE, message);
	}

	public AkIllegalStateException(String message, Throwable cause) {
		super(ErrorCode.ILLEGAL_STATE, message, cause);
	}
}
