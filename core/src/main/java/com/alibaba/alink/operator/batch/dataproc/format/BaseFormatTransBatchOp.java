package com.alibaba.alink.operator.batch.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;

/**
 * Transform vector to table columns. This transformer will map vector column to columns as designed.
 */
public class BaseFormatTransBatchOp<T extends BaseFormatTransBatchOp <T>> extends MapBatchOp <T> {

	private BaseFormatTransBatchOp() {
		this(null);
	}

	public BaseFormatTransBatchOp(FormatType fromFormat, FormatType toFormat, Params params) {
		this(
			(null == params ? new Params() : params)
				.set(FormatTransParams.FROM_FORMAT, fromFormat)
				.set(FormatTransParams.TO_FORMAT, toFormat)
		);
	}

	private BaseFormatTransBatchOp(Params params) {
		super(FormatTransMapper::new, params);
	}
}
