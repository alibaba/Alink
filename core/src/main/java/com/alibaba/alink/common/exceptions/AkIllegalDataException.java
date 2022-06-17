package com.alibaba.alink.common.exceptions;

public class AkIllegalDataException extends ExceptionWithErrorCode {
	public AkIllegalDataException(String message) {
		super(ErrorCode.ILLEGAL_DATA, message);
	}
}
