package com.alibaba.alink.common.exceptions;

public class AkUnimplementedOperationException extends ExceptionWithErrorCode {
	public AkUnimplementedOperationException(String message) {
		super(ErrorCode.UNIMPLEMENTED_OPERATION, message);
	}
}
