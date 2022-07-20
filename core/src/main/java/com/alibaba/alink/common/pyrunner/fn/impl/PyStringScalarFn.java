package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

@Internal
public class PyStringScalarFn extends BasePyScalarFn <String, PyScalarFnHandle <String>> {

	private final static Logger LOG = LoggerFactory.getLogger(PyStringScalarFn.class);

	public PyStringScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections.emptyMap());
	}

	public PyStringScalarFn(String name, String fnSpecJson,
							Map <String, String> runConfig) {
		super(name, fnSpecJson, String.class, runConfig);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.STRING;
	}

	public String eval(Object... args) {
		long start = System.currentTimeMillis();
		LOG.info("In Java eval {}", start);
		String result = runner.calc(args);
		long end = System.currentTimeMillis();
		LOG.info("Out Java eval {}", end);
		LOG.info("Elapsed time: {}", end - start);
		return result;
	}
}
