package com.alibaba.alink.params.dataproc.format;

public interface FromKvParams<T> extends
	HasKvCol <T>,
	HasKvColDelimiterDefaultAsComma <T>,
	HasKvValDelimiterDefaultAsColon <T> {
}