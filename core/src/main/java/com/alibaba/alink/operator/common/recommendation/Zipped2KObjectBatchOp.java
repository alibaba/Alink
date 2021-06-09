package com.alibaba.alink.operator.common.recommendation;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.recommendation.Zipped2KObjectParams;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Transform table format recommendation to json format.
 */
public class Zipped2KObjectBatchOp extends BatchOperator <Zipped2KObjectBatchOp>
	implements Zipped2KObjectParams <Zipped2KObjectBatchOp> {

	private static final long serialVersionUID = 453987106359846405L;

	public Zipped2KObjectBatchOp() {
		super(null);
	}

	public Zipped2KObjectBatchOp(Params params) {
		super(params);
	}

	@Override
	public Zipped2KObjectBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];
		TypeInformation <?> groupType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), this.getGroupCol());

		String[] selectedCols;
		String[] rewriteCols;
		if (getParams().contains(INFO_COLS)) {
			selectedCols = ArrayUtils.addAll(new String[] {getObjectCol()}, getInfoCols());
			rewriteCols = ArrayUtils.addAll(new String[] {getObjectCol()}, getInfoCols());
		} else {
			selectedCols = new String[] {getObjectCol()};
			rewriteCols = new String[] {getObjectCol()};
		}

		DataSet <Row> data = in
			.select(ArrayUtils.add(selectedCols, getGroupCol()))
			.getDataSet();

		DataSet <Row> res = data
			.groupBy(rewriteCols.length)
			.reduceGroup(new GroupReduceFunction <Row, Row>() {
				private static final long serialVersionUID = -894644555095876563L;

				@Override
				public void reduce(Iterable <Row> values, Collector <Row> out) {
					Object groupValue = null;
					Map <String, List <Object>> columnMajor = new TreeMap <>();

					for (Row row : values) {
						if (null == groupValue) {
							groupValue = row.getField(rewriteCols.length);
						}

						for (int i = 0; i < rewriteCols.length; ++i) {
							columnMajor.merge(
								rewriteCols[i],
								new ArrayList <>(Collections.singletonList(row.getField(i))),
								(a, b) -> {
									a.addAll(b);
									return a;
								}
							);
						}
					}

					out.collect(
						Row.of(
							groupValue,
							KObjectUtil.serializeKObject(
								columnMajor
							)
						)
					);
				}
			});

		this.setOutput(res, new String[] {this.getGroupCol(), this.getOutputCol()},
			new TypeInformation[] {groupType, Types.STRING});
		return this;
	}

}
