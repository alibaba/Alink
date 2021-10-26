package com.alibaba.alink.server.controller;

import javax.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BasicResponse implements Serializable {

	/**
	 * 成功
	 */
	public static String OK = "OK";

	/**
	 * 输入参数错误
	 */
	public static String INVALID_ARGUMENT = "INVALID_ARGUMENT";

	/**
	 * 没有权限
	 */
	public static String NO_PERMISSION = "NO_PERMISSION";

	/**
	 * 服务内部错误
	 */
	public static String INTERNAL_ERROR = "INTERNAL_ERROR";

	/**
	 * 返回值的状态信息，OK表示成功，其它失败
	 * <ol>
	 *  <li> INVALID_ARGUMENT:    invalid arguments </li>
	 *  <li> NO_PERMISSION:       no permission to do it </li>
	 *  <li> UNSUPPORTED:         not support this feature </li>
	 *  <li> ALREADY_BEEN_KILLED: this experiment has already been killed </li>
	 *  <li> RECYCLED:            this item has been recycled </li>
	 *  <li> INTERNAL_ERROR:      unknown error </li>
	 * </ol>
	 */
	@NotNull
	public String status;

	/**
	 * 请求出错时的错误描述信息
	 */
	protected String message = null;

	public BasicResponse(String status) {
		this.status = status;
	}

	public BasicResponse(String status, String message) {
		this.status = status;
		this.message = message;
	}

	public static BasicResponse OK() {
		return new BasicResponse(OK);
	}

	public static BasicResponse FAIL() {
		return new BasicResponse(INTERNAL_ERROR);
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
