package com.alibaba.alink.pipeline.dataproc.format;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.format.FormatTransMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.pipeline.MapTransformer;

public class BaseFormatTrans<T extends BaseFormatTrans <T>>
	extends MapTransformer <T> implements HasReservedColsDefaultAsNull<T> {
	private static final long serialVersionUID = 2874631150832168657L;

	public BaseFormatTrans(FormatType fromFormat, FormatType toFormat, Params params) {
		this(
			(null == params ? new Params() : params)
				.set(FormatTransParams.FROM_FORMAT, fromFormat)
				.set(FormatTransParams.TO_FORMAT, toFormat)
		);
	}

	private BaseFormatTrans(Params params) {
		super(FormatTransMapper::new, params);
	}
}
