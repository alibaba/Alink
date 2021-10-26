package com.alibaba.alink.server.excpetion;

public class InvalidEdgeIdException extends IllegalArgumentException {
	public InvalidEdgeIdException(Long edgeId) {
		super(String.format("Edge [%d] does not exist!", edgeId));
	}
}
