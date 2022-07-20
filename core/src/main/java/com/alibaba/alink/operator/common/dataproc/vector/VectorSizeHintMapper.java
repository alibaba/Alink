package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;

/**
 * This mapper checks the size of vector and give results as parameter define.
 */
public class VectorSizeHintMapper extends SISOMapper {
	private static final long serialVersionUID = 5056834356417351493L;
	private final int size;

	private final HandleInvalidMethod handleMethod;

	public VectorSizeHintMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.handleMethod = this.params.get(VectorSizeHintParams.HANDLE_INVALID);
		this.size = this.params.get(VectorSizeHintParams.SIZE);
	}

	@Override
	protected TypeInformation initOutputColType() {
		return AlinkTypes.VECTOR;
	}

	@Override
	protected Object mapColumn(Object input) throws Exception {
		Vector vec;
		switch (handleMethod) {
			case ERROR:
				if (input == null) {
					throw new AkIllegalDataException(
						"Got null vector in VectorSizeHint");
				} else {
					vec = VectorUtil.getVector(input);
					if (vec.size() == size) {
						return vec;
					} else {
						throw new AkIllegalOperatorParameterException(
							"VectorSizeHint : vec size (" + vec.size() + ") not equal param size (" + size + ").");
					}
				}
			case SKIP:
				if (input != null) {
					return VectorUtil.getVector(input);
				} else {
					return null;
				}
			default:
				throw new AkIllegalOperatorParameterException("Not support param " + handleMethod);
		}
	}
}
