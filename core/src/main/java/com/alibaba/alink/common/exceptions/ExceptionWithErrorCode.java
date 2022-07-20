package com.alibaba.alink.common.exceptions;

abstract class ExceptionWithErrorCode extends RuntimeException {
	private final ErrorCode errorCode;

	public ExceptionWithErrorCode(ErrorCode errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
	}

	public ExceptionWithErrorCode(ErrorCode errorCode, String message, Throwable cause) {
		super(message, cause);
		this.errorCode = errorCode;
	}

	@Override
	public String toString() {
		return String.format("%s: %s", errorCode, getLocalizedMessage());
	}
}
