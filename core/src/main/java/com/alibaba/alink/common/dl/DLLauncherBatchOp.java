package com.alibaba.alink.common.dl;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortDesc;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.dl.coding.ExampleCodingV2;
import com.alibaba.alink.common.dl.data.TFRecordReaderImpl;
import com.alibaba.alink.common.dl.data.TFRecordWriterImpl;
import com.alibaba.alink.common.dl.utils.DLLauncherUtils;
import com.alibaba.alink.common.dl.utils.DLTypeUtils;
import com.alibaba.alink.common.dl.utils.DLUtils;
import com.alibaba.alink.common.dl.utils.DataSetDiskDownloader;
import com.alibaba.alink.common.dl.utils.PythonFileUtils;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.utils.DataSetUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.FirstReducer;
import com.alibaba.alink.params.dl.DLLauncherParams;
import com.alibaba.flink.ml.tensorflow2.client.DLConfig;
import com.alibaba.flink.ml.util.MLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This operator sets up a DL cluster, launch python processes in Flink's task managers, feeds data to python processes
 * and collects data back.
 * <p>
 * This operator uses many of the technologies of the project: https://github.com/alibaba/flink-ai-extended
 */
@InputPorts(values = {
	@PortSpec(PortType.DATA),
	@PortSpec(value = PortType.DATA, desc = PortDesc.DL_BC_DATA, isRepeated = true)}
)
@OutputPorts(values = @PortSpec(PortType.DATA))
@Internal
public final class DLLauncherBatchOp extends BatchOperator <DLLauncherBatchOp>
	implements DLLauncherParams <DLLauncherBatchOp> {

	private static final Logger LOG = LoggerFactory.getLogger(DLLauncherBatchOp.class);

	public DLLauncherBatchOp() {
		this(new Params());
	}

	public DLLauncherBatchOp(Params params) {
		super(params);
	}

	private DLConfig setupDLConfig(TableSchema inputSchema, TableSchema outputSchema) {
		final int numWorkers = getNumWorkers();
		final int numPSs = getNumPSs();
		DLConfig config = new DLConfig(numWorkers, numPSs, new HashMap <>(), (String) null, getEntryFunc(), null);
		DLUtils.setExampleCodingType(config, inputSchema, outputSchema);

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
		DLUtils.safePutProperties(config, MLConstants.ML_RUNNER_CLASS, DLRunner.class.getCanonicalName());
		DLUtils.safePutProperties(config, MLConstants.CONFIG_STORAGE_TYPE, MLConstants.STORAGE_CUSTOM);
		DLUtils.safePutProperties(config, MLConstants.STORAGE_IMPL_CLASS, MemoryStorageImplV2.class.getName());

		// we use TFRecord as the default data format.
		DLUtils.safePutProperties(config, MLConstants.RECORD_READER_CLASS, TFRecordReaderImpl.class.getCanonicalName());
		DLUtils.safePutProperties(config, MLConstants.RECORD_WRITER_CLASS, TFRecordWriterImpl.class.getCanonicalName());
		DLUtils.safePutProperties(config, MLConstants.ENCODING_CLASS, ExampleCodingV2.class.getCanonicalName());
		DLUtils.safePutProperties(config, MLConstants.DECODING_CLASS, ExampleCodingV2.class.getCanonicalName());

		// There is a bug in TensorFlow 1.x, which must set the parallelism very early in the Python script.
		// (See: https://stackoverflow.com/questions/34426268/restricting-number-of-cores-used)
		// So, we set the argument in DLConfig, and handle it in Python side automatically.
		DLUtils.safePutProperties(config, DLConstants.INTRA_OP_PARALLELISM, String.valueOf(getIntraOpParallelism()));
		return config;
	}

	/**
	 * Redistribute the data set to first N partitions strictly.
	 */
	private DataSet <Row> dataSetFirstNPartitionStrictRebalance(DataSet <Row> input, final int numPartitions) {
		return input.mapPartition(new RichMapPartitionFunction <Row, Tuple2 <Integer, Row>>() {
			@Override
			public void mapPartition(Iterable <Row> values, Collector <Tuple2 <Integer, Row>> out) throws Exception {
				int pid = 0;
				Row lastEle = null;
				for (Row r : values) {
					lastEle = r;
					out.collect(Tuple2.of(pid, r));
					pid++;
					if (pid == numPartitions) {
						pid -= numPartitions;
					}
				}
				if (null != lastEle && pid != 0) {
					while (pid < numPartitions) {
						out.collect(Tuple2.of(pid, lastEle));
						pid++;
					}
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
	public DLLauncherBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = inputs[0];
		in = DLTypeUtils.doubleColumnsToFloat(in);

		setMLEnvironmentId(in.getMLEnvironmentId());

		ExecutionEnvironment env = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment();

		Tuple2 <Integer, Integer> numWorkersPSsTuple = DLLauncherUtils.adjustNumWorkersPSs(getNumWorkers(), getNumPSs(),
			env.getParallelism());
		setNumWorkers(numWorkersPSsTuple.f0);
		setNumPSs(numWorkersPSsTuple.f1);

		DataSet <Row> input = in.getDataSet();
		final int numWorkers = getNumWorkers();
		final int numPSs = getNumPSs();

		String outputSchemaStr = getOutputSchemaStr();
		TableSchema outputSchema = TableUtil.schemaStr2Schema(outputSchemaStr);
		DLConfig config = setupDLConfig(in.getSchema(), outputSchema);

		ExternalFilesConfig externalFiles = getUserFiles();

		List <String> filePaths = new ArrayList <>(externalFiles.getFilePaths());
		Map <String, String> scriptRenameMap = externalFiles.getFileRenameMap();

		String pythonEnv = getPythonEnv();
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

		if (numPSs > 0) {
			// move all data to workers but not parameter servers.
			input = dataSetFirstNPartitionStrictRebalance(input, numWorkers);
		} else {
			input = dataSetFirstNPartitionStrictRebalance(input, numWorkers + numPSs);
		}

		// When feeding a dataset to iteration, Flink may reset its parallelism to default parallelism (See
		// {@link GraphCreatingVisitor#preVisit})
		// Yet, it will not happen if the dataset is visited before the iterative dataset.
		// Adding a sink to the dataset seems to force the dataset to be visited earlier.
		// May need to confirm, but it works for now.
		input.output(new DiscardingOutputFormat <>());

		// In this MapPartition operator, python processes are launched, data are sent to
		// and collect from them.
		TableSchema ipPortSchema = new TableSchema(new String[] {"ip_port"}, new TypeInformation[] {Types.STRING});
		RichMapPartitionFunction <Row, Row> mapPartitionOp = new DLClusterMapPartitionFunc(
			config.getMlConfig(), in.getSchema(), outputSchema);

		// To setup a DL cluster, we need two steps:
		// Step A: collect IPs and ports of each tasks of a Flink MapPartition operator
		// Step B: broadcast above IPs and ports to downstream Flink MapPartition operator,
		//         then launch python processes in that operator.
		//
		// One important thing is that the tasks of the MapPartition operator in step A and step B
		// should be co-located. That is, tasks with same id in A and B should be deployed to the
		// same machine.
		//
		// To achieve task co-location, we relies on the iteration mechanism of FLink.
		// In Flink's iteration environment, the tasks of an operator are held fixed to the
		// same machine among different steps.
		//
		// Thus, we fire a two-step iteration, collects IPs and ports in step 1, and
		// launch python processes in step 2.
		DataSet <Row> state = DataSetUtil.createEmptyDataSet(env, outputSchema, ipPortSchema);
		IterativeDataSet <Row> loop = state.iterate(2);

		MapPartitionOperator <Row, Row> result = input
			.mapPartition(mapPartitionOp)
			.withBroadcastSet(loop, DLConstants.IP_PORT_BC_NAME)
			.name("DL_CLUSTER");

		// inputs[1:] will be broadcast to each workers, and writen to files
		// in each tasks' local disk, so that they can be accessed by python processes.
		for (int i = 1; i < inputs.length; i++) {
			result = result.withBroadcastSet(inputs[i].getDataSet(), DLConstants.BC_NAME_PREFIX + i);
		}

		result
			.withBroadcastSet(extractTensorShapes(in.getDataSet(), in.getColNames()), DLConstants.BC_NAME_TENSOR_SHAPES)
			.withBroadcastSet(extractTensorTypes(in.getDataSet(), in.getColNames()), DLConstants.BC_NAME_TENSOR_TYPES);

		BatchOperator <?> downloaderOp = DataSetDiskDownloader.downloadFilesWithRename(getMLEnvironmentId(),
			filePaths,
			scriptRenameMap);
		result = result.withBroadcastSet(downloaderOp.getDataSet(), DLConstants.BC_NAME_DOWNLOAD_PATHS);

		DataSet <Row> output = loop.closeWith(result);
		DataSet <Object> barrier = output.mapPartition(new MapPartitionFunction <Row, Object>() {
			@Override
			public void mapPartition(Iterable <Row> values, Collector <Object> out) throws Exception {
				LOG.info("killing DL tasks------");
				if (OsType.WINDOWS.equals(OsUtils.getSystemType())) {
					String winStr ="for /f \"skip=1 tokens=1,2 delims=, \" %a in ('tasklist /fi \" " +
							"IMAGENAME eq python.exe\" /FO csv ')  do (  wmic process where processid" +
							"=%b get commandline | findstr startup.py | taskkill /pid %b -f )";
					Runtime.getRuntime().exec(new String[] {"cmd.exe", winStr}, null, null);
				} else {
					String shStr = "ps -ef | grep " + "\"temp_[0-9*]_.*/startup.py\""
							+ " | awk '{print $2}' | xargs kill -9";
					Runtime.getRuntime().exec(new String[] {"/bin/bash", "-c", shStr}, null, null);
				}
			}
		});

		DataSet <Row> forceOutput = output.map(new MapFunction <Row, Row>() {
			@Override
			public Row map(Row value) throws Exception {
				return value;
			}
		}).withBroadcastSet(barrier, "barrier");
		setOutput(DataSetUtil.removeLastColumn(forceOutput), outputSchema);
		return this;
	}

	private DataSet <Map <String, long[]>> extractTensorShapes(DataSet <Row> dataSet, String[] colNames) {
		return dataSet.reduceGroup(new FirstReducer <>(1))
			.flatMap(new FlatMapFunction <Row, Map <String, long[]>>() {
				@Override
				public void flatMap(Row value, Collector <Map <String, long[]>> out) throws Exception {
					Map <String, long[]> tensorShapeMap = new HashMap <>();
					for (int i = 0; i < colNames.length; i += 1) {
						if (value.getField(i) instanceof Tensor) {
							tensorShapeMap.put(colNames[i], ((Tensor <?>) value.getField(i)).shape());
						}
					}
					out.collect(tensorShapeMap);
				}
			});
	}

	private DataSet <Map <String, String>> extractTensorTypes(DataSet <Row> dataSet, String[] colNames) {
		return dataSet.reduceGroup(new FirstReducer <>(1))
			.flatMap(new FlatMapFunction <Row, Map <String, String>>() {
				@Override
				public void flatMap(Row value, Collector <Map <String, String>> out) {
					Map <String, String> tensorShapeMap = new HashMap <>();
					for (int i = 0; i < colNames.length; i += 1) {
						if (value.getField(i) instanceof Tensor) {
							tensorShapeMap.put(colNames[i],
								((Tensor <?>) value.getField(i)).getType().name().toLowerCase());
						}
					}
					out.collect(tensorShapeMap);
				}
			});
	}
}

