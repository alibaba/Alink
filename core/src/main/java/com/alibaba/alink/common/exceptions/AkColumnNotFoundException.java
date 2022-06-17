package com.alibaba.alink.common.exceptions;

public class AkColumnNotFoundException extends ExceptionWithErrorCode {
	public AkColumnNotFoundException(String message) {
		super(ErrorCode.COLUMN_NOT_FOUND, message);
	}
}
