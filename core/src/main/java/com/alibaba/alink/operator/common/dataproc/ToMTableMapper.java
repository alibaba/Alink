package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.mapper.SISOMapper;
import com.alibaba.alink.params.dataproc.ToMTableParams;
import com.alibaba.alink.params.shared.HasHandleInvalid.HandleInvalidMethod;

public class ToMTableMapper extends SISOMapper {
	private final HandleInvalidMethod handleInvalidMethod;

	public ToMTableMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		handleInvalidMethod = params.get(ToMTableParams.HANDLE_INVALID);
	}

	@Override
	protected Object mapColumn(Object input) {
		if (null == input) {
			return null;
		}

		MTable mTable = null;
		try {
			if (input instanceof String) {
				mTable = MTable.fromJson((String) input);
			} else if (input instanceof MTable) {
				mTable = (MTable) input;
			} else {
				throw new RuntimeException("Input type not support yet.");
			}
		} catch (Exception ex) {
			switch (handleInvalidMethod) {
				case ERROR:
					throw ex;
				case SKIP:
					break;
				default:
					throw new UnsupportedOperationException();
			}
		}
		return mTable;
	}

	@Override
	protected TypeInformation <?> initOutputColType() {
		return AlinkTypes.M_TABLE;
	}
}
