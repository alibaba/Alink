package com.alibaba.alink.params.dataproc.format;


public interface ToKvParams<T> extends
	HasKvCol <T>,
	HasKvColDelimiterDefaultAsComma <T>,
	HasKvValDelimiterDefaultAsColon <T> {
}