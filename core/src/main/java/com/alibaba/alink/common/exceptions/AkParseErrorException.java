package com.alibaba.alink.common.exceptions;

public class AkParseErrorException extends ExceptionWithErrorCode {
	public AkParseErrorException(String message) {
		super(ErrorCode.PARSE_ERROR, message);
	}

	public AkParseErrorException(String message, Throwable cause) {
		super(ErrorCode.PARSE_ERROR, message, cause);
	}
}
