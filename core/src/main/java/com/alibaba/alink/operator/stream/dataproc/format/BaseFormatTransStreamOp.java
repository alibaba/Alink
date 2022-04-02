package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.SelectedColsWithFirstInputSpec;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

/**
 * The base class of transformation between csv, json, kv, vector and table types.
 */
@SelectedColsWithFirstInputSpec
@NameCn("")
public class BaseFormatTransStreamOp<T extends BaseFormatTransStreamOp <T>>
	extends MapStreamOp <T> implements HasReservedColsDefaultAsNull<T> {

	private static final long serialVersionUID = 1812617669373659300L;

	private BaseFormatTransStreamOp() {
		this(null);
	}

	public BaseFormatTransStreamOp(FormatType fromFormat, FormatType toFormat, Params params) {
		this(
			(null == params ? new Params() : params)
				.set(FormatTransParams.FROM_FORMAT, fromFormat)
				.set(FormatTransParams.TO_FORMAT, toFormat)
		);
	}

	private BaseFormatTransStreamOp(Params params) {
		super(FormatTransMapper::new, params);
	}
}
