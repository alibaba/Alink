package com.alibaba.alink.server.excpetion;

public class InvalidNodeIdException extends IllegalArgumentException {
	public InvalidNodeIdException(Long nodeId) {
		super(String.format("Node [%d] does not exist!", nodeId));
	}
}
