package com.alibaba.alink.common.pyrunner.fn.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.pyrunner.fn.BasePyScalarFn;
import com.alibaba.alink.common.pyrunner.fn.PyScalarFnHandle;
import com.alibaba.alink.common.pyrunner.fn.conversion.MTableWrapper;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class PyMTableScalarFn extends BasePyScalarFn <MTableWrapper, PyScalarFnHandle <MTableWrapper>> {
	private final static Logger LOG = LoggerFactory.getLogger(PyMTableScalarFn.class);

	public PyMTableScalarFn(String name, String fnSpecJson) {
		this(name, fnSpecJson, Collections. <String, String>emptyMap()::getOrDefault);
	}

	public PyMTableScalarFn(String name, String fnSpecJson,
							SerializableBiFunction <String, String, String> runConfigGetter) {
		super(name, fnSpecJson, MTableWrapper.class, runConfigGetter);
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return AlinkTypes.M_TABLE;
	}

	public MTable eval(Object... args) {
		return runner.calc(args).getJavaObject();
	}
}
