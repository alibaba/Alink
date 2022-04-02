package com.alibaba.alink.operator.stream.dataproc.format;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.format.AnyToTripleFlatMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.dataproc.format.ToTripleParams;

/**
 * The base class of transform other types to triple.
 */

@NameCn("")
class AnyToTripleStreamOp<T extends AnyToTripleStreamOp <T>> extends FlatMapStreamOp <T>
	implements ToTripleParams <T> {

	private static final long serialVersionUID = -4107279247585307842L;

	public AnyToTripleStreamOp() {
		this(null);
	}

	public AnyToTripleStreamOp(FormatType formatType, Params params) {
		this(
			(null == params ? new Params() : params)
				.set(FormatTransParams.FROM_FORMAT, formatType)
		);
	}

	public AnyToTripleStreamOp(Params params) {
		super(AnyToTripleFlatMapper::new, params);
	}
}
