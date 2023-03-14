package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.pyrunner.fn.conversion.VectorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class PyVectorScalarFn extends BasePyScalarFn <VectorWrapper, PyScalarFnHandle <VectorWrapper>> {
	private final static Logger LOG = LoggerFactory.getLogger(PyVectorScalarFn.class);

	public PyVectorScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections.emptyMap());
	}

	public PyVectorScalarFn(String name, String fnSpecJson, Map <String, String> runConfig) {
		super(name, fnSpecJson, VectorWrapper.class, runConfig);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return AlinkTypes.VECTOR;
	}

	public Vector eval(Object... args) {
		return (Vector) runner.calc(args).getJavaObject();
	}
}
