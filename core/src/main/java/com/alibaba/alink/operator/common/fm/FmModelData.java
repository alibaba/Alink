package com.alibaba.alink.operator.common.fm;

import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;

import java.io.Serializable;

/**
 * Fm model data.
 */
public class FmModelData implements Serializable {

	private static final long serialVersionUID = 7452756889593215611L;
	public String vectorColName = null;
	public String[] featureColNames = null;
	public String labelColName = null;
	public FmDataFormat fmModel;
	public int vectorSize;
	public int[] dim;
	public double[] regular;
	public Object[] labelValues = null;
	public Task task;
	public double[] convergenceInfo;
}
