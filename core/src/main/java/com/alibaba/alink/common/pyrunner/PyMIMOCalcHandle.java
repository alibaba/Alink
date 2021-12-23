package com.alibaba.alink.common.pyrunner;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.pyrunner.PyMIMOCalcRunner.PyListRowOutputCollector;

import java.util.List;
import java.util.Map;

/**
 * A handle type for MIMO calculation, i.e. inputs and outputs are both a list of rows.
 * <p>
 * As {@link List} and {@link Row} types are not auto-convertible by Py4J, inputs are passed to Python side as a
 * two-dimensional array of {@link Object}s. Outputs are collected through {@link PyListRowOutputCollector}, instead of
 * returned as return values.
 */
public interface PyMIMOCalcHandle extends PyObjHandle {

	void setCollector(PyListRowOutputCollector collector);

	void calc(Object[][] inputs);

	void calc(Map <String, String> conf, Object[][] input1, Object[][] input2);
}
