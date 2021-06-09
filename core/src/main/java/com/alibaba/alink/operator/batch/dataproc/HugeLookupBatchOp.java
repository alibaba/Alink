package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.LookupParams;

/**
 * Huge Lookup BatchOp, find values by column value
 */
public class HugeLookupBatchOp extends BatchOperator <HugeLookupBatchOp>
	implements LookupParams <HugeLookupBatchOp> {

	public HugeLookupBatchOp() {
		this(new Params());
	}

	public HugeLookupBatchOp(Params params) {
		super(params);
	}

	@Override
	public HugeLookupBatchOp linkFrom(BatchOperator <?>... inputs) {
		checkOpSize(2, inputs);

		BatchOperator <?> model = inputs[0];
		BatchOperator <?> data = inputs[1];

		final String[] mapKeyColNames = getMapKeyCols();
		final String[] mapValueColNames = getMapValueCols();
		final String[] selectedColNames = getSelectedCols();
		final String[] reservedColNames = getReservedCols();
		String[] outputColNames = getOutputCols();

		TableSchema modelSchema = model.getSchema();
		TableSchema dataSchema = data.getSchema();

		if (modelSchema.getFieldNames().length != 2 && (mapKeyColNames == null || mapValueColNames == null)) {
			throw new RuntimeException("LookUp err : mapKeyCols and mapValueCols should set in parameters.");
		}

		final int[] selectedColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, selectedColNames);
		;
		final int[] mapKeyColIndices = (mapKeyColNames != null) ?
			TableUtil.findColIndicesWithAssertAndHint(modelSchema, mapKeyColNames) : new int[] {0};
		final int[] mapValueColIndices = (mapValueColNames != null) ?
			TableUtil.findColIndicesWithAssertAndHint(modelSchema, mapValueColNames) : new int[] {1};

		for (int i = 0; i < selectedColNames.length; ++i) {
			if (mapKeyColNames != null && mapValueColNames != null) {
				if (TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColNames[i])
					!= TableUtil.findColTypeWithAssertAndHint(modelSchema, mapKeyColNames[i])) {
					throw new IllegalArgumentException("Data types are not match. selected column type is "
						+ TableUtil.findColTypeWithAssertAndHint(dataSchema, selectedColNames[i])
						+ " , and the map key column type is "
						+ TableUtil.findColTypeWithAssertAndHint(modelSchema, mapKeyColNames[i])
					);
				}
			}
		}

		if (null == outputColNames) {
			outputColNames = mapValueColNames;
		}

		final TypeInformation <?>[] outputColTypes = (mapValueColNames == null)
			? TableUtil.findColTypesWithAssertAndHint(modelSchema, new String[] {modelSchema.getFieldNames()[1]})
			: TableUtil.findColTypesWithAssertAndHint(modelSchema, mapValueColNames);

		final OutputColsHelper predResultColsHelper
			= new OutputColsHelper(dataSchema, outputColNames, outputColTypes, reservedColNames);

		DataSet <Row> result = data
			.getDataSet()
			.leftOuterJoin(
				model.getDataSet(),
				JoinHint.REPARTITION_SORT_MERGE
			)
			.where(selectedColIndices)
			.equalTo(mapKeyColIndices)
			.with(new JoinFunction <Row, Row, Row>() {
				@Override
				public Row join(Row first, Row second) {
					Row result = new Row(mapValueColIndices.length);

					if (second != null) {
						for (int i = 0; i < mapValueColIndices.length; ++i) {
							result.setField(i, second.getField(mapValueColIndices[i]));
						}
					}

					return predResultColsHelper.getResultRow(first, result);
				}
			});

		setOutput(result, predResultColsHelper.getResultSchema());

		return this;
	}
}
