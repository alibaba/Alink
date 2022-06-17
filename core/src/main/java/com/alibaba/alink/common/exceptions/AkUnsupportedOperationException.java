package com.alibaba.alink.common.exceptions;

public class AkUnsupportedOperationException extends ExceptionWithErrorCode {
	public AkUnsupportedOperationException(String message) {
		super(ErrorCode.UNSUPPORTED_OPERATION, message);
	}
}
