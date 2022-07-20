package com.alibaba.alink.operator.common.dataproc.tensor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.tensor.TensorReshapeParams;

public class TensorReshapeMapper extends SISOMapper {
	private static final long serialVersionUID = -3091895748206887001L;
	private long[] size;

	public TensorReshapeMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		Integer[] sizeTmp = this.params.get(TensorReshapeParams.SIZE);
		this.size = new long[sizeTmp.length];
		for (int i = 0; i < sizeTmp.length; i++) {
			this.size[i] = sizeTmp[i];
		}
	}

	@Override
	protected Object mapColumn(Object input) {
		Tensor <?> tensor = TensorUtil.getTensor(input);
		return tensor.reshape(new Shape(size));
	}

	@Override
	protected TypeInformation initOutputColType() {
		return AlinkTypes.TENSOR;
	}
}
