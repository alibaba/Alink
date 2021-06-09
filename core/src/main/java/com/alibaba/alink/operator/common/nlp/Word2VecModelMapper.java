package com.alibaba.alink.operator.common.nlp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.operator.common.dataproc.AggLookupModelMapper;
import com.alibaba.alink.params.dataproc.AggLookupParams;

import com.alibaba.alink.params.nlp.HasPredMethod;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public class Word2VecModelMapper extends AggLookupModelMapper {
	private static final long serialVersionUID = -8885570542417759579L;

	public Word2VecModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params.set(AggLookupParams.CLAUSE,
			params.get(HasPredMethod.PRED_METHOD) + "(" + params.get(HasSelectedCol.SELECTED_COL) + ") as "
				+ (params.contains(HasOutputCol.OUTPUT_COL) ? params.get(HasOutputCol.OUTPUT_COL) :
				params.get(HasSelectedCol.SELECTED_COL))));
	}
}
