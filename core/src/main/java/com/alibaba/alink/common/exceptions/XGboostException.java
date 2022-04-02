package com.alibaba.alink.common.exceptions;

public class XGboostException extends Exception {

	public XGboostException(String message) {
		super(message);
	}

	public XGboostException(String message, Throwable cause) {
		super(message, cause);
	}

	public XGboostException(Throwable cause) {
		super(cause);
	}
}
