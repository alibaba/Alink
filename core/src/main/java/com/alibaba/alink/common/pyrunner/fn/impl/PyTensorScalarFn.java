package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.pyrunner.fn.conversion.TensorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class PyTensorScalarFn extends BasePyScalarFn <TensorWrapper, PyScalarFnHandle <TensorWrapper>> {
	private final static Logger LOG = LoggerFactory.getLogger(PyTensorScalarFn.class);

	public PyTensorScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections.emptyMap());
	}

	public PyTensorScalarFn(String name, String fnSpecJson, Map <String, String> runConfig) {
		super(name, fnSpecJson, TensorWrapper.class, runConfig);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return AlinkTypes.TENSOR;
	}

	public Tensor <?> eval(Object... args) {
		return runner.calc(args).getJavaObject();
	}
}
