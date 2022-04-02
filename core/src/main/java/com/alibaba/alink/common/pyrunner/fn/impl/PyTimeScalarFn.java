package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.util.Collections;

@Internal
public class PyTimeScalarFn extends BasePyScalarFn <Integer, PyScalarFnHandle <Integer>> {

	public PyTimeScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyTimeScalarFn(String name, String fnSpecJson,
						  SerializableBiFunction <String, String, String> runConfigGetter) {
		super(name, fnSpecJson, int.class, runConfigGetter);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.SQL_TIME;
	}

	public int eval(Object... args) {
		return runner.calc(args);
	}
}
