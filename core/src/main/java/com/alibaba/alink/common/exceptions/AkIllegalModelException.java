package com.alibaba.alink.common.exceptions;

public class AkIllegalModelException extends ExceptionWithErrorCode {
	public AkIllegalModelException(String message) {
		super(ErrorCode.ILLEGAL_MODEL, message);
	}

	public AkIllegalModelException(String message, Throwable cause) {
		super(ErrorCode.ILLEGAL_MODEL, message, cause);
	}
}
