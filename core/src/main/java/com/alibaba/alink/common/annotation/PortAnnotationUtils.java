package com.alibaba.alink.common.annotation;

import org.apache.flink.api.java.tuple.Tuple2;

public class PortAnnotationUtils {
	public static Tuple2 <InputPorts, OutputPorts> getInputOutputPorts(Class <?> clz) {
		InputPorts declaredInputPorts = clz.getDeclaredAnnotation(InputPorts.class);
		OutputPorts declaredOutputPorts = clz.getDeclaredAnnotation(OutputPorts.class);
		if (null != declaredInputPorts && null != declaredOutputPorts) {
			return Tuple2.of(declaredInputPorts, declaredOutputPorts);
		}
		InputPorts inputPorts = clz.getAnnotation(InputPorts.class);
		OutputPorts outputPorts = clz.getAnnotation(OutputPorts.class);
		if (null != inputPorts && null != outputPorts) {
			return Tuple2.of(inputPorts, outputPorts);
		}
		return null;
	}
}
