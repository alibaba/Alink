package com.alibaba.alink.common.exceptions;

public class AkIllegalOperation extends ExceptionWithErrorCode {
	public AkIllegalOperation(String message) {
		super(ErrorCode.ILLEGAL_OPERATION, message);
	}
}
