package com.alibaba.alink.common.pyrunner.fn;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.pyrunner.fn.impl.PyBigDecimalScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyBooleanScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyByteScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyDateScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyDoubleScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyFloatScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyIntegerScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyLongScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyMTableScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyShortScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyStringScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyTensorScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyTimeScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyTimestampScalarFn;
import com.alibaba.alink.common.pyrunner.fn.impl.PyVectorScalarFn;
import com.alibaba.alink.common.utils.Functional.SerializableBiFunction;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PyFnFactory {

	private static final Map <String, Class <?>> SCALAR_FN_MAPPING = new HashMap <>();

	static {
		SCALAR_FN_MAPPING.put("BOOL", PyBooleanScalarFn.class);
		SCALAR_FN_MAPPING.put("BOOLEAN", PyBooleanScalarFn.class);
		SCALAR_FN_MAPPING.put("BYTE", PyByteScalarFn.class);
		SCALAR_FN_MAPPING.put("TINYINT", PyByteScalarFn.class);
		SCALAR_FN_MAPPING.put("SHORT", PyShortScalarFn.class);
		SCALAR_FN_MAPPING.put("SMALLINT", PyShortScalarFn.class);
		SCALAR_FN_MAPPING.put("INT", PyIntegerScalarFn.class);
		SCALAR_FN_MAPPING.put("INTEGER", PyIntegerScalarFn.class);
		SCALAR_FN_MAPPING.put("BIGINT", PyLongScalarFn.class);
		SCALAR_FN_MAPPING.put("LONG", PyLongScalarFn.class);
		SCALAR_FN_MAPPING.put("FLOAT", PyFloatScalarFn.class);
		SCALAR_FN_MAPPING.put("DOUBLE", PyDoubleScalarFn.class);
		SCALAR_FN_MAPPING.put("BIG_DEC", PyBigDecimalScalarFn.class);
		SCALAR_FN_MAPPING.put("BIGDECIMAL", PyBigDecimalScalarFn.class);
		SCALAR_FN_MAPPING.put("DECIMAL", PyBigDecimalScalarFn.class);
		SCALAR_FN_MAPPING.put("STRING", PyStringScalarFn.class);
		SCALAR_FN_MAPPING.put("VARCHAR", PyStringScalarFn.class);
		SCALAR_FN_MAPPING.put("TIMESTAMP", PyTimestampScalarFn.class);
		SCALAR_FN_MAPPING.put("DATETIME", PyTimestampScalarFn.class);
		SCALAR_FN_MAPPING.put("DATE", PyDateScalarFn.class);
		SCALAR_FN_MAPPING.put("TIME", PyTimeScalarFn.class);
		SCALAR_FN_MAPPING.put("VECTOR", PyVectorScalarFn.class);
		SCALAR_FN_MAPPING.put("TENSOR", PyTensorScalarFn.class);
		SCALAR_FN_MAPPING.put("MTABLE", PyMTableScalarFn.class);
	}

	public static ScalarFunction makeScalarFn(String name, String resultType, String fnSpecJson) {
		return PyFnFactory.makeScalarFn(name, resultType, fnSpecJson,
			Collections. <String, String>emptyMap()::getOrDefault);
	}

	public static ScalarFunction makeScalarFn(String name, String resultType, String fnSpecJson,
											  SerializableBiFunction <String, String, String> runConfigGetter) {
		if (!SCALAR_FN_MAPPING.containsKey(resultType.toUpperCase())) {
			throw new RuntimeException(String.format("Invalid result type %s for Python scalar function.", resultType));
		}
		Class <?> clz = SCALAR_FN_MAPPING.get(resultType.toUpperCase());
		try {
			Constructor <?> constructor = clz.getConstructor(String.class, String.class, SerializableBiFunction.class);
			return (ScalarFunction) constructor.newInstance(name, fnSpecJson, runConfigGetter);
		} catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(String.format("Unable to construct %s.", clz.getSimpleName()), e);
		}
	}

	public static TableFunction <Row> makeTableFn(String name, String fnSpecJson, List <String> resultTypes) {
		return PyFnFactory.makeTableFn(name, fnSpecJson, resultTypes.toArray(new String[0]));
	}

	public static TableFunction <Row> makeTableFn(String name, String fnSpecJson, String[] resultTypes) {
		return PyFnFactory.makeTableFn(name, fnSpecJson, resultTypes,
			Collections. <String, String>emptyMap()::getOrDefault);
	}

	public static TableFunction <Row> makeTableFn(String name, String fnSpecJson, String[] resultTypes,
												  SerializableBiFunction <String, String, String> runConfigGetter) {
		return new PyTableFn(name, fnSpecJson, resultTypes, runConfigGetter);
	}
}
