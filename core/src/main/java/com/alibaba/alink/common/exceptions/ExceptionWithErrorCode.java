package com.alibaba.alink.common.exceptions;

abstract class ExceptionWithErrorCode extends RuntimeException {
	private final ErrorCode errorCode;
	private final String message;

	public ExceptionWithErrorCode(ErrorCode errorCode, String message) {
		this.errorCode = errorCode;
		this.message = message;
	}

	@Override
	public String toString() {
		return String.format("%s: %s", errorCode, message);
	}
}
