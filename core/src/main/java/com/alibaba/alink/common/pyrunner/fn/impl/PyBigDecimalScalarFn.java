package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

@Internal
public class PyBigDecimalScalarFn extends BasePyScalarFn <BigDecimal, PyScalarFnHandle <BigDecimal>> {

	public PyBigDecimalScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections.emptyMap());
	}

	public PyBigDecimalScalarFn(String name, String fnSpecJson, Map <String, String> runConfig) {
		super(name, fnSpecJson, BigDecimal.class, runConfig);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.BIG_INT;
	}

	public BigDecimal eval(Object... args) {
		return runner.calc(args);
	}
}
