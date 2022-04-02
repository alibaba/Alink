package com.alibaba.alink.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@ParamSelectColumnSpec(name = "reservedCols", portIndices = 0, defaultSelectAll = true)
public @interface ReservedColsWithFirstInputSpec {
}
