package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;

/**
 * The base class of transformation between csv, json, kv, vector and table types.
 */
public class BaseFormatTransBatchOp<T extends BaseFormatTransBatchOp <T>>
	extends MapBatchOp <T> implements HasReservedColsDefaultAsNull<T> {

	private static final long serialVersionUID = 2376903138165544996L;

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
