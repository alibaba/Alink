package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.NumericalTypeCastMapper;
import com.alibaba.alink.operator.common.outlier.IForestDetector.IForestTrain;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector.IForestModel;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector.IForestModelDataConverter;
import com.alibaba.alink.operator.common.outlier.OutlierUtil;
import com.alibaba.alink.operator.local.AlinkLocalSession;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.utils.MTableParallelUtil;
import com.alibaba.alink.params.dataproc.HasTargetType.TargetType;
import com.alibaba.alink.params.dataproc.NumericalTypeCastParams;
import com.alibaba.alink.params.outlier.IForestTrainParams;
import com.alibaba.alink.params.outlier.WithMultiVarParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@InputPorts(values = @PortSpec(value = PortType.DATA))
@OutputPorts(values = @PortSpec(value = PortType.MODEL))
@NameCn("IForest模型异常检测训练")
@NameEn("IForest model outlier")
public class IForestModelOutlierTrainLocalOp
	extends LocalOperator <IForestModelOutlierTrainLocalOp>
	implements IForestTrainParams <IForestModelOutlierTrainLocalOp> {

	private static final Logger LOG = LoggerFactory.getLogger(IForestModelOutlierTrainLocalOp.class);

	private static final double LOG2 = Math.log(2);

	public IForestModelOutlierTrainLocalOp() {
		this(null);
	}

	public IForestModelOutlierTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	public IForestModelOutlierTrainLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		final Params params = getParams().clone();
		final int numTrees = getNumTrees();
		final int subsamplingSize = getSubsamplingSize();
		final int numThreads = MTableParallelUtil.getNumThreads(params);

		final String[] colNames = in.getColNames();
		final TypeInformation <?>[] colTypes = in.getColTypes();

		MTable input = in.getOutputTable();

		if (params.contains(WithMultiVarParams.VECTOR_COL)) {

			final int vectorIndex = TableUtil.findColIndexWithAssertAndHint(
				in.getSchema(), params.get(WithMultiVarParams.VECTOR_COL)
			);

			int[] maxVectorSizeBuf = new int[numThreads];

			MTableParallelUtil.traverse(
				input,
				numThreads,
				(mTable, index, start, end) -> {
					int localMaxVectorSize = -1;
					for (int i = start; i < end; ++i) {
						localMaxVectorSize = Math.max(
							localMaxVectorSize,
							OutlierUtil.vectorSize(
								VectorUtil.getVector(input.getRow(i).getField(vectorIndex))
							)
						);
					}

					maxVectorSizeBuf[index] = localMaxVectorSize;
				}
			);

			params.set(OutlierUtil.MAX_VECTOR_SIZE, Arrays.stream(maxVectorSizeBuf).max().orElse(-1));
		}

		params.set(
			IForestTrainParams.SUBSAMPLING_SIZE,
			Math.min(params.get(IForestTrainParams.SUBSAMPLING_SIZE), input.getNumRow())
		);

		final TaskRunner taskRunner = new TaskRunner();
		final IForestModel model = new IForestModel();

		model.meta = params.clone();
		model.trees.addAll(Collections.nCopies(numTrees, null));

		for (int i = 0; i < numThreads; ++i) {
			final int start = (int) AlinkLocalSession.DISTRIBUTOR.startPos(i, numThreads, numTrees);
			final int end = (int) AlinkLocalSession.DISTRIBUTOR.localRowCnt(i, numThreads, numTrees) + start;

			taskRunner.submit(() -> {
				for (int j = start; j < end; ++j) {
					try {
						MTable series = OutlierUtil.getMTable(
							input.sampleWithSize(params.get(IForestTrainParams.SUBSAMPLING_SIZE),
								ThreadLocalRandom.current()),
							params
						);

						int numRows = series.getNumRow();

						List <Row> rows = new ArrayList <>(numRows);

						final NumericalTypeCastMapper numericalTypeCastMapper = new NumericalTypeCastMapper(
							series.getSchema(),
							new Params()
								.set(NumericalTypeCastParams.SELECTED_COLS, series.getColNames())
								.set(NumericalTypeCastParams.TARGET_TYPE, TargetType.DOUBLE)
						);

						for (int k = 0; k < numRows; ++k) {
							rows.add(numericalTypeCastMapper.map(series.getRow(k)));
						}

						series = new MTable(rows, series.getSchemaStr());

						if (numRows > 0) {
							int heightLimit = (int) Math.ceil(Math.log(Math.min(rows.size(), subsamplingSize)) / LOG2);

							IForestTrain iForestTrain = new IForestTrain(params);

							model.trees.set(j, iForestTrain.iTree(series, heightLimit, ThreadLocalRandom.current()));
						}
					} catch (Exception ex) {
						LOG.error(String.format("Create iForest %d Failed.", j), ex);
					}
				}
			});
		}

		taskRunner.join();

		final IForestModelDataConverter iForestModelDataConverter = new IForestModelDataConverter();

		final List <Row> serialized = new ArrayList <>();

		iForestModelDataConverter.save(
			model,
			new Collector <Row>() {
				@Override
				public void collect(Row record) {
					serialized.add(record);
				}

				@Override
				public void close() {
					// pass
				}
			}
		);

		setOutputTable(new MTable(serialized, iForestModelDataConverter.getModelSchema()));

		return this;
	}
}
