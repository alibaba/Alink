package com.alibaba.alink.common.exceptions;

public class AkUnclassifiedErrorException extends ExceptionWithErrorCode {
	public AkUnclassifiedErrorException(String message) {
		super(ErrorCode.UNCLASSIFIED_ERROR, message);
	}
}
