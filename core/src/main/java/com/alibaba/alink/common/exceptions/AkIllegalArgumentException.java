package com.alibaba.alink.common.exceptions;

public class AkIllegalArgumentException extends ExceptionWithErrorCode {
	public AkIllegalArgumentException(String message) {
		super(ErrorCode.ILLEGAL_ARGUMENT, message);
	}
}
