package com.alibaba.alink.operator.common.tensorflow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.plugin.DLPredictorService;
import com.alibaba.alink.common.dl.plugin.TFPredictorClassLoaderFactory;
import com.alibaba.alink.common.mapper.FlatMapper;
import com.alibaba.alink.common.utils.OutputColsHelper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dl.HasInferBatchSizeDefaultAs256;
import com.alibaba.alink.params.dl.HasModelPath;
import com.alibaba.alink.params.tensorflow.savedmodel.BaseTFSavedModelPredictParams;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.GRAPH_DEF_TAG_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INPUT_SIGNATURE_DEFS_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTER_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.INTRA_OP_PARALLELISM_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.MODEL_PATH_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_SIGNATURE_DEFS_KEY;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.OUTPUT_TYPE_CLASSES;
import static com.alibaba.alink.operator.common.tensorflow.TFSavedModelConstants.SIGNATURE_DEF_KEY_KEY;

/**
 * A FlatMapper version of {@link BaseTFSavedModelPredictRowMapper}.
 * <p>
 * `modelPath` must be a local path which is obtained from `params` or setter.
 */
public class BaseTFSavedModelPredictRowFlatMapper extends FlatMapper implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BaseTFSavedModelPredictRowFlatMapper.class);

	private static final long QUEUE_OFFER_TIMEOUT_MS = 50;

	private final TFPredictorClassLoaderFactory factory;

	private final String graphDefTag;
	private final String signatureDefKey;
	private final int[] tfInputColIds;
	private final String[] tfOutputCols;
	private final Class <?>[] tfOutputColTypeClasses;
	private final OutputColsHelper outputColsHelper;
	private String[] inputSignatureDefs;
	private String[] outputSignatureDefs;
	private String modelPath;

	private int batchSize = 256;
	private AtomicBoolean stopInference;
	private ArrayBlockingQueue <Pair <Row, Collector <Row>>> queue;
	private ExecutorService executorService;
	private Future <?> inferenceRunnerFuture;
	private DLPredictorService predictor;

	public BaseTFSavedModelPredictRowFlatMapper(TableSchema dataSchema, Params params) {
		this(dataSchema, params, new TFPredictorClassLoaderFactory());
	}

	public BaseTFSavedModelPredictRowFlatMapper(TableSchema dataSchema, Params params,
												TFPredictorClassLoaderFactory factory) {
		super(dataSchema, params);

		this.factory = factory;

		graphDefTag = params.get(BaseTFSavedModelPredictParams.GRAPH_DEF_TAG);
		signatureDefKey = params.get(BaseTFSavedModelPredictParams.SIGNATURE_DEF_KEY);

		String[] tfInputCols = params.get(BaseTFSavedModelPredictParams.SELECTED_COLS);
		if (null == tfInputCols) {
			tfInputCols = dataSchema.getFieldNames();
		}
		tfInputColIds = TableUtil.findColIndicesWithAssertAndHint(dataSchema, tfInputCols);
		inputSignatureDefs = params.get(BaseTFSavedModelPredictParams.INPUT_SIGNATURE_DEFS);
		if (null == inputSignatureDefs) {
			inputSignatureDefs = tfInputCols;
		}

		Preconditions.checkArgument(params.contains(BaseTFSavedModelPredictParams.OUTPUT_SCHEMA_STR),
			"Must set outputSchemaStr.");
		String tfOutputSchemaStr = params.get(BaseTFSavedModelPredictParams.OUTPUT_SCHEMA_STR);
		TableSchema tfOutputSchema = TableUtil.schemaStr2Schema(tfOutputSchemaStr);
		tfOutputCols = tfOutputSchema.getFieldNames();
		outputSignatureDefs = params.get(BaseTFSavedModelPredictParams.OUTPUT_SIGNATURE_DEFS);
		if (null == outputSignatureDefs) {
			outputSignatureDefs = tfOutputCols;
		}
		TypeInformation <?>[] tfOutputColTypes = tfOutputSchema.getFieldTypes();
		tfOutputColTypeClasses = Arrays.stream(tfOutputColTypes)
			.map(TypeInformation::getTypeClass)
			.toArray(Class[]::new);

		outputColsHelper = new OutputColsHelper(dataSchema,
			tfOutputCols, tfOutputColTypes,
			params.get(BaseTFSavedModelPredictParams.RESERVED_COLS));

		if (params.contains(HasModelPath.MODEL_PATH)) {
			modelPath = params.get(HasModelPath.MODEL_PATH);
		}

		if (params.contains(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE)) {
			batchSize = params.get(HasInferBatchSizeDefaultAs256.INFER_BATCH_SIZE);
		}
	}

	public BaseTFSavedModelPredictRowFlatMapper setModelPath(String modelPath) {
		this.modelPath = modelPath;
		return this;
	}

	@Override
	public TableSchema getOutputSchema() {
		return outputColsHelper.getResultSchema();
	}

	@Override
	public void open() {
		Preconditions.checkArgument(modelPath != null, "Model path is not set.");
		Integer intraOpParallelism = params.contains(BaseTFSavedModelPredictParams.INTRA_OP_PARALLELISM)
			? params.get(BaseTFSavedModelPredictParams.INTRA_OP_PARALLELISM)
			: null;

		try {
			predictor = (DLPredictorService) factory.create()
				.loadClass("com.alibaba.alink.common.dl.plugin.TFPredictorServiceImpl")
				.getConstructor()
				.newInstance();
		} catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException ex) {
			throw new RuntimeException(ex);
		}

		Map <String, Object> config = new HashMap <>();
		config.put(MODEL_PATH_KEY, modelPath);
		config.put(GRAPH_DEF_TAG_KEY, graphDefTag);
		config.put(SIGNATURE_DEF_KEY_KEY, signatureDefKey);
		config.put(INPUT_SIGNATURE_DEFS_KEY, inputSignatureDefs);
		config.put(OUTPUT_SIGNATURE_DEFS_KEY, outputSignatureDefs);
		config.put(OUTPUT_TYPE_CLASSES, tfOutputColTypeClasses);
		config.put(INTRA_OP_PARALLELISM_KEY, intraOpParallelism);
		config.put(INTER_OP_PARALLELISM_KEY, null);

		predictor.open(config);

		queue = new ArrayBlockingQueue <>(batchSize);
		stopInference = new AtomicBoolean(false);
		executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque <>());
		inferenceRunnerFuture = executorService.submit(new InferenceRunner());
	}

	@Override
	public void close() {
		stopInference.set(true);
		try {
			inferenceRunnerFuture.get();
			executorService.shutdown();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}
		try {
			predictor.close();
		} catch (Exception e) {
			throw new RuntimeException("Failed to close predictor", e);
		}
	}

	@Override
	public void flatMap(Row row, Collector <Row> collector) throws Exception {
		while (!queue.offer(Pair.of(row, collector), QUEUE_OFFER_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
			// When the queue is full, the status of inference runner is checked.
			try {
				inferenceRunnerFuture.get(0, TimeUnit.MILLISECONDS);
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			} catch (TimeoutException ignored) {
			}
		}
	}

	private class InferenceRunner implements Runnable {
		@Override
		public void run() {
			List <Pair <Row, Collector <Row>>> rowCollectors = new ArrayList <>();
			while (!stopInference.get() || !queue.isEmpty()) {
				Pair <Row, Collector <Row>> rowCollector = null;
				try {
					rowCollector = queue.poll(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (null != rowCollector) {
					rowCollectors.add(rowCollector);
				}
				if (rowCollectors.size() == batchSize) {
					processRows(rowCollectors);
					rowCollectors.clear();
				}
			}
			if (rowCollectors.size() > 0) {
				processRows(rowCollectors);
				rowCollectors.clear();
			}
		}

		public void processRows(List <Pair <Row, Collector <Row>>> rowCollectors) {
			long startTime = System.currentTimeMillis();
			int currentBatchSize = rowCollectors.size();
			List <List <?>> valueLists = new ArrayList <>();

			for (int tfInputColId : tfInputColIds) {
				List <Object> values = new ArrayList <>();
				for (Pair <Row, Collector <Row>> rowCollector : rowCollectors) {
					Row tfInput = rowCollector.getLeft();
					values.add(tfInput.getField(tfInputColId));
				}
				valueLists.add(values);
			}

			List <List <?>> outputs = predictor.predictRows(valueLists, currentBatchSize);

			Row[] predictions = IntStream.range(0, currentBatchSize)
				.mapToObj(i -> new Row(tfOutputCols.length))
				.toArray(Row[]::new);

			for (int k = 0; k < outputs.size(); k += 1) {
				List <?> values = outputs.get(k);
				for (int i = 0; i < currentBatchSize; i += 1) {
					predictions[i].setField(k, values.get(i));
				}
			}

			for (int i = 0; i < currentBatchSize; i += 1) {
				Pair <Row, Collector <Row>> rowCollector = rowCollectors.get(i);
				Row output = outputColsHelper.getResultRow(rowCollector.getLeft(), predictions[i]);
				rowCollector.getRight().collect(output);
			}
			LOG.info("{} items cost {} ms.", currentBatchSize, System.currentTimeMillis() - startTime);
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.printf("%s items cost %s ms.%n", currentBatchSize,
					System.currentTimeMillis() - startTime);
			}
		}
	}
}
