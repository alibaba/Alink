package com.alibaba.alink.common.exceptions;

public class MTableSerializerException extends RuntimeException {

	public MTableSerializerException(String message) {
		super(message);
	}

	public MTableSerializerException(String message, Throwable cause) {
		super(message, cause);
	}
}
