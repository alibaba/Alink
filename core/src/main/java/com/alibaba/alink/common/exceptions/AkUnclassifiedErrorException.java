package com.alibaba.alink.common.exceptions;

public class AkUnclassifiedErrorException extends ExceptionWithErrorCode {
	public AkUnclassifiedErrorException(String message) {
		super(ErrorCode.UNCLASSIFIED_ERROR, message);
	}

	public AkUnclassifiedErrorException(String message, Throwable cause) {
		super(ErrorCode.UNCLASSIFIED_ERROR, message, cause);
	}
}
