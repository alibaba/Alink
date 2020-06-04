//
// Copyright (c) 2014, Alibaba Inc.
// All rights reserved.
//
// Author: Yan Huang <allison.hy@alibaba-inc.com>
// Created: 6/12/18
// Description:
//

package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransMapper;
import com.alibaba.alink.operator.common.dataproc.format.FormatTransParams;
import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.operator.common.dataproc.format.FormatWriter;
import com.alibaba.alink.params.dataproc.format.FromTripleParams;
import com.alibaba.alink.params.dataproc.format.HasHandleInvalidDefaultAsError;
import com.alibaba.alink.params.dataproc.format.HasHandleInvalidDefaultAsError.HandleInvalid;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class TripleToAnyBatchOp<T extends TripleToAnyBatchOp<T>> extends BatchOperator <T>
	implements FromTripleParams<T> {

	private static final long serialVersionUID = 6283495106807306943L;


	public TripleToAnyBatchOp(FormatType toFormat, Params params) {
		super((null == params ? new Params() : params).set(FormatTransParams.TO_FORMAT, toFormat));
	}

	@Override
	public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		String rowColName = getTripleRowCol();
		String columnColName = getTripleColCol();
		String valueColName = getTripleValCol();
		TypeInformation rowType = TableUtil.findColType(in.getSchema(), rowColName);
		DataSet <Tuple3 <Comparable, Object, Object>> tuple3
			= in.select(new String[] {rowColName, columnColName, valueColName})
			.getDataSet()
			.map(
				new MapFunction <Row, Tuple3 <Comparable, Object, Object>>() {
					@Override
					public Tuple3 <Comparable, Object, Object> map(Row value) throws Exception {
						return new Tuple3 <>((Comparable) value.getField(0), value.getField(1), value.getField(2));
					}
				}
			);
		DataSet <Row> dataSet = tuple3
			.groupBy(0)
			.reduceGroup(new ToAny(getParams()));

		Tuple3 <FormatWriter, String[], TypeInformation[]> t3To
			= FormatTransMapper.initFormatWriter(getParams(), null);
		String[] outputColNames = t3To.f1;
		TypeInformation[] outputColTypes = t3To.f2;

		this.setOutput(
			dataSet,
			ArrayUtils.addAll(new String[] {rowColName}, outputColNames),
			ArrayUtils.addAll(new TypeInformation <?>[] {rowType}, outputColTypes)
		);
		return (T) this;
	}

	public static class ToAny extends RichGroupReduceFunction<Tuple3 <Comparable, Object, Object>, Row> {
		private static final long serialVersionUID = 4128130689819716473L;
		private final Params params;
		FormatWriter formatWriter;
		private HandleInvalid handleInvalid;

		public ToAny(Params params) {
			this.params = params;
			this.handleInvalid = params.get(HasHandleInvalidDefaultAsError.HANDLE_INVALID);
		}

		@Override
		public void open(Configuration parameters) {
			formatWriter = FormatTransMapper.initFormatWriter(params, null).f0;
		}

		@Override
		public void reduce(Iterable <Tuple3 <Comparable, Object, Object>> iterable, Collector <Row> out)
			throws Exception {
			Object rowItem = null;
			HashMap <String, String> bufMap = new HashMap <>();
			for (Tuple3 <Comparable, Object, Object> item : iterable) {
				rowItem = item.f0;
				bufMap.put(item.f1.toString(), item.f2.toString());
			}

			Tuple2 <Boolean, Row> t2 = formatWriter.write(bufMap);
			if (!t2.f0) {
				if (handleInvalid.equals(HandleInvalid.ERROR)) {
					throw new RuntimeException("Fail to convert: " + JsonConverter.toJson(bufMap));
				} else {
					return;
				}
			}

			Row row = new Row(1 + t2.f1.getArity());
			row.setField(0, rowItem);
			for (int i = 0; i < t2.f1.getArity(); i++) {
				row.setField(i + 1, t2.f1.getField(i));
			}

			out.collect(row);
		}
	}

}
