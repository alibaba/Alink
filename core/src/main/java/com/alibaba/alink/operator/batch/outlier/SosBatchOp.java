package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.SOSImpl;
import com.alibaba.alink.params.outlier.SosParams;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Stochastic Outlier Selection algorithm.
 */
public final class SosBatchOp
	extends BatchOperator <SosBatchOp>
	implements SosParams <SosBatchOp> {

	private static final long serialVersionUID = -7535849193750624523L;

	public SosBatchOp() {
		this(new Params());
	}

	public SosBatchOp(Params params) {
		super(params);
	}

	@Override
	public SosBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		final String vectorColName = getVectorCol();
		final String predResultColName = getPredictionCol();
		final int vectorColIdx = TableUtil.findColIndexWithAssertAndHint(in.getColNames(), vectorColName);

		DataSet <Tuple2 <Integer, Row>> pointsWithIndex = DataSetUtils
			.zipWithIndex(in.getDataSet())
			.map(new MapFunction <Tuple2 <Long, Row>, Tuple2 <Integer, Row>>() {
				private static final long serialVersionUID = -2866816204744880078L;

				@Override
				public Tuple2 <Integer, Row> map(Tuple2 <Long, Row> in) throws Exception {
					return new Tuple2 <>(in.f0.intValue(), in.f1);
				}
			});

		DataSet <Tuple2 <Integer, DenseVector>> sosInput = pointsWithIndex
			.map(new MapFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, DenseVector>>() {
				private static final long serialVersionUID = 5798841020968290284L;

				@Override
				public Tuple2 <Integer, DenseVector> map(Tuple2 <Integer, Row> in) throws Exception {
					Vector vec = VectorUtil.getVector(in.f1.getField(vectorColIdx));
					if (null == vec) {
						return new Tuple2 <>(in.f0, null);
					} else {
						return new Tuple2 <>(in.f0,
							(vec instanceof DenseVector) ? (DenseVector) vec : ((SparseVector) vec).toDenseVector());
					}
				}
			});

		SOSImpl sos = new SOSImpl(this.getParams());
		DataSet <Tuple2 <Integer, Double>> outlierProb = sos.outlierSelection(sosInput);

		DataSet <Row> output = outlierProb
			.join(pointsWithIndex)
			.where(0)
			.equalTo(0)
			.with(new JoinFunction <Tuple2 <Integer, Double>, Tuple2 <Integer, Row>, Row>() {
				private static final long serialVersionUID = 7086848937713200592L;

				@Override
				public Row join(Tuple2 <Integer, Double> in1, Tuple2 <Integer, Row> in2) throws Exception {
					Row row = new Row(in2.f1.getArity() + 1);
					for (int i = 0; i < in2.f1.getArity(); i++) {
						row.setField(i, in2.f1.getField(i));
					}
					row.setField(in2.f1.getArity(), in1.f1);
					return row;
				}
			})
			.returns(new RowTypeInfo(ArrayUtils.add(in.getColTypes(), Types.DOUBLE)));

		this.setOutput(output, ArrayUtils.add(in.getColNames(), predResultColName));

		return this;
	}

}
