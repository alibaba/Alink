package com.alibaba.alink.common.exceptions;

public class ScanFailRuntimeException extends RuntimeException {

	public ScanFailRuntimeException(String message) {
		super(message);
	}

	public ScanFailRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}
}
