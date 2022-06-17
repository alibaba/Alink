package com.alibaba.alink.common.dl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream.ConnectedIterativeStreams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.dl.DLEnvConfig.Version;
import com.alibaba.alink.common.dl.utils.DLTypeUtils;
import com.alibaba.alink.common.dl.utils.DLUtils;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.plugin.ResourcePluginFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.dl.DLLauncherParams;
import com.alibaba.flink.ml.tensorflow2.client.DLConfig;
import com.alibaba.flink.ml.util.MLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.alibaba.alink.common.dl.utils.DLLauncherUtils.adjustNumWorkersPSs;

/**
 * This operator supports running custom DL scripts for stream datasets.
 * <p>
 * "cluster" field in DL_CONFIG is set, so distributed training is supported. Right now, this operator cannot link from
 * BatchOperators.
 * <p>
 * {@link DLStreamCoFlatMapFunc} is the core of this operator.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.DATA))
@Internal
public final class DLLauncherStreamOp extends StreamOperator <DLLauncherStreamOp>
	implements DLLauncherParams <DLLauncherStreamOp> {

	private static final Logger LOG = LoggerFactory.getLogger(DLLauncherStreamOp.class);

	// All IPs and ports are supposed to be collected within this time limit.
	public static long DL_CLUSTER_START_TIME = 3 * 60 * 1000; // milliseconds

	private final ResourcePluginFactory factory;

	public DLLauncherStreamOp() {
		this(new Params());
	}

	public DLLauncherStreamOp(Params params) {
		super(params);
		factory = new ResourcePluginFactory();
	}

	private DLConfig setupDLConfig(TableSchema inputSchema, TableSchema outputSchema) {
		final int numWorkers = getNumWorkers();
		final int numPSs = getNumPSs();
		DLConfig config = new DLConfig(numWorkers, numPSs, new HashMap <>(), (String) null, getEntryFunc(), null);
		DLUtils.setExampleCodingType(config, inputSchema, outputSchema);

		DLUtils.safePutProperties(config, MLConstants.ML_RUNNER_CLASS, DLRunner.class.getCanonicalName());
		DLUtils.safePutProperties(config, MLConstants.CONFIG_STORAGE_TYPE, MLConstants.STORAGE_CUSTOM);
		DLUtils.safePutProperties(config, MLConstants.STORAGE_IMPL_CLASS, MemoryStorageImplV2.class.getName());
		if (!StringUtils.isNullOrWhitespaceOnly(getPythonEnv())) {
			DLUtils.safePutProperties(config, DLConstants.PYTHON_ENV, getPythonEnv());
		} else if (null != getEnvVersion()) {
			DLUtils.safePutProperties(config, DLConstants.ENV_VERSION, getEnvVersion().name());
		}
		DLUtils.safePutProperties(config, DLConstants.ENTRY_SCRIPT, getMainScriptFile());
		DLUtils.safePutProperties(config, DLConstants.ENTRY_FUNC, getEntryFunc());
		DLUtils.safePutProperties(config, DLConstants.USER_DEFINED_PARAMS, getUserParams());
		DLUtils.safePutProperties(config, DLConstants.NUM_WORKERS, String.valueOf(numWorkers));
		DLUtils.safePutProperties(config, DLConstants.NUM_PSS, String.valueOf(numPSs));
		DLUtils.safePutProperties(config, MLConstants.NODE_IDLE_TIMEOUT, String.valueOf(5 * 1000));

		// There is a bug in TensorFlow 1.x, which must set the parallelism very early in the Python script.
		// (See: https://stackoverflow.com/questions/34426268/restricting-number-of-cores-used)
		// So, we set the argument in DLConfig, and handle it in Python side automatically.
		DLUtils.safePutProperties(config, DLConstants.INTRA_OP_PARALLELISM, String.valueOf(getIntraOpParallelism()));
		return config;
	}

	/**
	 * Redistribute the data set to first #numWorkers partitions.
	 * By the way, send one dummy element to each ps node from each partition to avoid that one partition has no data.
	 */
	private DataStream <Row> dataStreamFirstNPartitionRebalance(DataStream <Row> input, final int numWorkers, final int numPSs) {
		return input
			.flatMap(new RichFlatMapFunction <Row, Tuple2 <Integer, Row>>() {

				private static final long serialVersionUID = 5072779969295321676L;

				@Override
				public void open(Configuration parameters) throws Exception {
					this.idx = -1;
				}

				int idx;
				boolean firstItem = true;

				@Override
				public void flatMap(Row value, Collector <Tuple2 <Integer, Row>> out) throws Exception {
					idx ++;
					idx = idx >= numWorkers? idx - numWorkers: idx;
					out.collect(Tuple2.of(idx, value));
					if (firstItem) {
						for (int i = 0; i < numPSs; i ++) {
							out.collect(Tuple2.of(numWorkers + i, value));
						}
						firstItem = false;
					}
				}
			})
			.partitionCustom(new Partitioner <Integer>() {
				private static final long serialVersionUID = -44838855219045312L;

				@Override
				public int partition(Integer key, int nPart) {
					return key % nPart;
				}
			}, 0)
			.map(new MapFunction <Tuple2 <Integer, Row>, Row>() {
				private static final long serialVersionUID = 5543012093523253627L;

				@Override
				public Row map(Tuple2 <Integer, Row> value) throws Exception {
					return value.f1;
				}
			});
	}

	@Override
	public DLLauncherStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = inputs[0];
		in = DLTypeUtils.doubleColumnsToFloat(in);
		setMLEnvironmentId(in.getMLEnvironmentId());

		Tuple2 <Integer, Integer> numWorkersPSsTuple = adjustNumWorkersPSs(getNumWorkers(), getNumPSs(),
			MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment().getParallelism());
		setNumWorkers(numWorkersPSsTuple.f0);
		setNumPSs(numWorkersPSsTuple.f1);

		DataStream <Row> input = in.getDataStream();
		final int numWorkers = getNumWorkers();
		final int numPSs = getNumPSs();

		String outputSchemaStr = getOutputSchemaStr();
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		DLConfig config = setupDLConfig(in.getSchema(), outputSchema);

		ExternalFilesConfig externalFiles = getUserFiles();
		Set <String> filePaths = externalFiles.getFilePaths();
		Map <String, String> fileRenameMap = externalFiles.getFileRenameMap();

		String pythonEnv = getPythonEnv();
		if (StringUtils.isNullOrWhitespaceOnly(pythonEnv)) {
			Version envVersion = getEnvVersion();
			DLUtils.safePutProperties(config, DLConstants.ENV_VERSION, envVersion.name());
		} else {
			String dirName;
			if (!StringUtils.isNullOrWhitespaceOnly(pythonEnv)) {
				if (PythonFileUtils.isLocalFile(pythonEnv)) {
					// should be a directory
					dirName = pythonEnv;
				} else {
					filePaths.add(pythonEnv);
					dirName = PythonFileUtils.getCompressedFileName(pythonEnv);
					DLUtils.safePutProperties(config, DLConstants.PYTHON_ENV, dirName);
				}
				DLUtils.safePutProperties(config, DLConstants.PYTHON_ENV, dirName);
			}
		}
		DLUtils.safePutProperties(config, DLConstants.EXTERNAL_FILE_CONFIG_JSON, externalFiles.toJson());

		input = dataStreamFirstNPartitionRebalance(input, numWorkers, numPSs);

		ConnectedIterativeStreams <Row, Row> iteration = input
			.map(new MapFunction <Row, Row>() {
				@Override
				public Row map(Row value) throws Exception {
					return Row.of(0, value);
				}
			})
			.iterate(DL_CLUSTER_START_TIME)
			.withFeedbackType(TypeInformation.of(new TypeHint <Row>() {}));

		DataStream <Row> iterationBody = iteration
			.flatMap(new DLStreamCoFlatMapFunc(config.getMlConfig(), numWorkers, numPSs, factory))
			.name("DL_CLUSTER");

		DataStream <Row> ipPortsStream = iterationBody.filter(new FilterFunction <Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return (Integer) value.getField(0) < 0;
			}
		}).partitionCustom(new Partitioner <Integer>() {
			@Override
			public int partition(Integer key, int numPartitions) {
				return key;
			}
		}, new KeySelector <Row, Integer>() {
			@Override
			public Integer getKey(Row value) throws Exception {
				return (Integer) value.getField(1);
			}
		});

		DataStream <Row> output = iterationBody.filter(new FilterFunction <Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return (Integer) value.getField(0) >= 0;
			}
		}).map(new MapFunction <Row, Row>() {
			@Override
			public Row map(Row value) throws Exception {
				return (Row) value.getField(1);
			}
		});

		iteration.closeWith(ipPortsStream);

		setOutput(output, outputSchema);
		return this;
	}
}
