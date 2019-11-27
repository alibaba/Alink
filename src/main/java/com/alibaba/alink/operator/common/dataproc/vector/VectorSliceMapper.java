package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;

/**
 * This mapper maps vector to a thinner one with special indices.
 */
public class VectorSliceMapper extends SISOMapper {

	private int[] indices;

	/**
	 * Constructor.
	 * @param dataSchema the data schema.
	 * @param params     the params.
	 */
	public VectorSliceMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.indices = this.params.get(VectorSliceParams.INDICES);
	}

	@Override
	protected Object mapColumn(Object input) {
		if (input == null) {
			return null;
		}

		Vector vec = VectorUtil.getVector(input);
		return vec.slice(indices);
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

}
