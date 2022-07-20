package com.alibaba.alink.common.exceptions;

public class AkNullPointerException extends ExceptionWithErrorCode {
	public AkNullPointerException(String message) {
		super(ErrorCode.NULL_POINTER, message);
	}
}
