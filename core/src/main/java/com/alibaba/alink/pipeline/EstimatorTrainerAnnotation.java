package com.alibaba.alink.pipeline;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface EstimatorTrainerAnnotation {

	String estimatorName();

}
