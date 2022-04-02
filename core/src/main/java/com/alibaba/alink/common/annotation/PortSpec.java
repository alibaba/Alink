package com.alibaba.alink.common.annotation;

/**
 * Specification of a input or output port.
 */
public @interface PortSpec {
	// Data type represented by the port
	PortType value();

	// Operator type can be linked from
	OpType opType() default OpType.SAME;

	boolean isOptional() default false;

	boolean isRepeated() default false;

	// Port description
	PortDesc desc() default PortDesc.EMPTY;

	// Suggested operators to link from/to, depending on the port is input or output
	Class <?>[] suggestions() default {};

	enum OpType {
		// same as the operator type
		SAME,
		// batch
		BATCH,
		// stream
		STREAM,
		// batch and stream
		BOTH
	}
}
