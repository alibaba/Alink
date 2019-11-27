package com.alibaba.alink.common.comqueue;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

/**
 * This function is executed at the end of last iteration, mainly to retrieve results
 */
public abstract class CompleteResultFunction implements Serializable {

	public abstract List <Row> calc(ComContext context);

}
