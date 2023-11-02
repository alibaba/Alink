package com.alibaba.alink.python.executables;

import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.utils.WithTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.BaseSinkBatchOp;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;
import com.alibaba.alink.operator.batch.source.DataSetWrapperBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.batch.utils.MTableSerializeBatchOp;
import com.alibaba.alink.operator.batch.utils.TensorSerializeBatchOp;
import com.alibaba.alink.operator.batch.utils.UDFBatchOp;
import com.alibaba.alink.operator.batch.utils.UDTFBatchOp;
import com.alibaba.alink.operator.batch.utils.VectorSerializeBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.BaseSinkStreamOp;
import com.alibaba.alink.operator.stream.source.BaseSourceStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.source.NumSeqSourceStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.operator.stream.utils.MTableSerializeStreamOp;
import com.alibaba.alink.operator.stream.utils.TensorSerializeStreamOp;
import com.alibaba.alink.operator.stream.utils.UDFStreamOp;
import com.alibaba.alink.operator.stream.utils.UDTFStreamOp;
import com.alibaba.alink.operator.stream.utils.VectorSerializeStreamOp;
import com.alibaba.alink.params.ModelStreamScanParams;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.pipeline.LocalPredictable;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.PipelineStageBase;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.tuning.BaseGridSearch;
import com.alibaba.alink.pipeline.tuning.BaseRandomSearch;
import com.alibaba.alink.pipeline.tuning.TuningEvaluator;
import com.alibaba.alink.python.utils.ParamUtil;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import org.apache.commons.lang3.StringUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generate Python code for all operators.
 */
public class GeneratePyOp {

	final static List <Class <?>> INTERFACE_CANDIDATES = Arrays.asList(
		WithModelInfoBatchOp.class, WithTrainInfo.class, ExtractModelInfoBatchOp.class,
		HasLazyPrintModelInfo.class, HasLazyPrintTrainInfo.class, LocalPredictable.class,
		ModelStreamScanParams.class, EvaluationMetricsCollector.class
	);

	final static String ENCODING_LINE = "# -*- coding: utf-8 -*-";

	/**
	 * Operators should not be visible in Python code.
	 */
	static Set <Class <?>> IGNORE_OP_SET = new HashSet <>(Arrays.asList(
		MemSourceBatchOp.class, MemSourceStreamOp.class,
		NumSeqSourceBatchOp.class, NumSeqSourceStreamOp.class,
		TableSourceBatchOp.class, TableSourceStreamOp.class,
		UDFBatchOp.class, UDFStreamOp.class,
		UDTFBatchOp.class, UDTFStreamOp.class,
		VectorSerializeBatchOp.class, VectorSerializeStreamOp.class,
		TensorSerializeBatchOp.class, TensorSerializeStreamOp.class,
		MTableSerializeBatchOp.class, MTableSerializeStreamOp.class,
		DataSetWrapperBatchOp.class,
		Pipeline.class, PipelineModel.class
	));

	static int MAX_CLASS_ONE_FILE = 1 << 28;

	static List <String> BATCH_IMPORT_LINES = Arrays.asList(
		"from ..base import BatchOperator, BaseSinkBatchOp",
		"from ..mixins import WithTrainInfo, EvaluationMetricsCollector, ExtractModelInfoBatchOp, WithModelInfoBatchOp"
	);

	static List <String> STREAM_IMPORT_LINES = Arrays.asList(
		"from ..base import StreamOperator, BaseSinkStreamOp, BaseModelStreamOp",
		"from ...common.types.bases.model_stream_scan_params import ModelStreamScanParams");

	static List <String> PIPELINE_IMPORT_LINES = Arrays.asList(
		"from ..base import Estimator, Transformer, Model, TuningEvaluator",
		"from ..tuning.base import BaseGridSearch, BaseRandomSearch",
		"from ..mixins import HasLazyPrintModelInfo, HasLazyPrintTrainInfo",
		"from ..local_predictor import LocalPredictable",
		"from ...common.types.bases.model_stream_scan_params import ModelStreamScanParams");

	static Map <Type, Class <?>[]> TYPE_BASES_CLASSES = new HashMap <>();
	static Map <Type, String> TYPE_FILENAME_PREFIX = new HashMap <>();
	static Map <Type, List <String>> TYPE_IMPORT_LINES = new HashMap <>();

	static {
		TYPE_BASES_CLASSES.put(Type.BATCH, new Class[] {BatchOperator.class});
		TYPE_BASES_CLASSES.put(Type.STREAM, new Class[] {StreamOperator.class});
		TYPE_BASES_CLASSES.put(Type.PIPELINE, new Class[] {PipelineStageBase.class, TuningEvaluator.class});

		TYPE_FILENAME_PREFIX.put(Type.BATCH, "batch_op_");
		TYPE_FILENAME_PREFIX.put(Type.STREAM, "stream_op_");
		TYPE_FILENAME_PREFIX.put(Type.PIPELINE, "pipeline_op_");

		TYPE_IMPORT_LINES.put(Type.BATCH, BATCH_IMPORT_LINES);
		TYPE_IMPORT_LINES.put(Type.STREAM, STREAM_IMPORT_LINES);
		TYPE_IMPORT_LINES.put(Type.PIPELINE, PIPELINE_IMPORT_LINES);
	}

	static String calcOpType(Class <?> cls) {
		if (BaseSourceBatchOp.class.isAssignableFrom(cls) || BaseSourceStreamOp.class.isAssignableFrom(cls)) {
			return "SOURCE";
		}
		if (BaseSinkBatchOp.class.isAssignableFrom(cls) || BaseSinkStreamOp.class.isAssignableFrom(cls)) {
			return "SINK";
		}
		return "FUNCTION";
	}

	static String calcPyBaseCls(Class <?> cls) {
		if (StreamOperator.class.isAssignableFrom(cls)) {
			try {
				if (checkHasModel(cls.getName())) {
					return "BaseModelStreamOp";
				}
				if (BaseSinkStreamOp.class.isAssignableFrom(cls)) {
					return "BaseSinkStreamOp";
				}
				return "StreamOperator";
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else if (BatchOperator.class.isAssignableFrom(cls)) {
			if (BaseSinkBatchOp.class.isAssignableFrom(cls)) {
				return "BaseSinkBatchOp";
			}
			return "BatchOperator";
		} else {
			if (TuningEvaluator.class.isAssignableFrom(cls)) {
				return "TuningEvaluator";
			} else if (BaseGridSearch.class.isAssignableFrom(cls)) {
				return "BaseGridSearch";
			} else if (BaseRandomSearch.class.isAssignableFrom(cls)) {
				return "BaseRandomSearch";
			} else if (ModelBase.class.isAssignableFrom(cls)) {
				return "Model";
			} else if (TransformerBase.class.isAssignableFrom(cls)) {
				return "Transformer";
			} else {
				return "Estimator";
			}
		}
	}

	static List <String> calcPyInterfaces(Class <?> cls) {
		return INTERFACE_CANDIDATES.stream()
			.filter(d -> d.isAssignableFrom(cls))
			.map(Class::getSimpleName)
			.collect(Collectors.toList());
	}

	static String formatParamName(String name) {
		return StringUtils.capitalize(name);
	}

	static String makeClassMethod(ParamInfo <?> info) {
		StringBuilder sb = new StringBuilder();
		sb.append("    def set").append(formatParamName(info.getName())).append("(self, val):\n");
		sb.append("        return self._add_param('").append(info.getName()).append("', val)\n");
		return sb.toString();
	}

	static String calc(Class <?> cls, String name) {
		List <ParamInfo> paramList = ParamUtil.getParametersByOperator(cls).stream()
			.filter(d -> !d.getName().startsWith("lazyPrint"))
			.collect(Collectors.toList());
		StringBuilder sb = new StringBuilder();
		String basePyCls = calcPyBaseCls(cls);
		List <String> basePyInterfaces = calcPyInterfaces(cls);
		basePyInterfaces.add(0, basePyCls);

		sb.append("class ").append(name).append("(").append(String.join(", ", basePyInterfaces)).append("):").append(
			"\n");
		sb.append("    CLS_NAME = '").append(cls.getName()).append("'\n");
		sb.append("    OP_TYPE = '").append(calcOpType(cls)).append("'\n");
		sb.append("\n");
		sb.append("    def __init__(self, *args, **kwargs):").append("\n");
		sb.append("        kwargs['CLS_NAME'] = self.CLS_NAME\n");
		sb.append("        kwargs['OP_TYPE'] = self.OP_TYPE\n");
		sb.append("        super(").append(name).append(", self).__init__(*args, **kwargs)").append("\n");
		sb.append("        pass").append("\n");

		final HashSet <String> names = new HashSet <>();
		for (ParamInfo <?> info : paramList) {
			if (names.contains(info.getName())) {
				continue;
			}
			if (HasMLEnvironmentId.ML_ENVIRONMENT_ID.equals(info)) {
				continue;
			}
			names.add(info.getName());
			sb.append("\n");
			sb.append(makeClassMethod(info));
		}
		return sb.toString();
	}

	static boolean shouldGenerate(Class <?> cls) {
		return !Modifier.isAbstract(cls.getModifiers())
			&& cls.getTypeParameters().length <= 0
			&& !IGNORE_OP_SET.contains(cls);
	}

	static List <String> genOpsLines(List <Class <?>> ops) {
		List <String> lines = new ArrayList <>();
		ops.stream()
			.filter(GeneratePyOp::shouldGenerate)
			.sorted(Comparator.comparing(Class::getSimpleName))
			.forEach(cls -> {
				lines.add("\n");
				lines.add(calc(cls, cls.getSimpleName()));
			});
		return lines;
	}

	static void writeSingleFile(String filename, List <String> importLines, List <String> opLines)
		throws FileNotFoundException, UnsupportedEncodingException {
		try (PrintStream ps = new PrintStream(new FileOutputStream(filename), true, "utf-8")) {
			ps.println(ENCODING_LINE);
			ps.println();
			for (String line : importLines) {
				ps.println(line);
			}
			ps.println();
			for (String line : opLines) {
				ps.println(line);
			}
			ps.flush();
		}
	}

	static void genTypeInfoFolder(String folder, Type type) throws IOException {
		final List <Class <?>> all = listSubtypeClasses(TYPE_BASES_CLASSES.get(type))
			.stream()
			.filter(GeneratePyOp::shouldGenerate)
			.sorted(Comparator.comparing(Class::getSimpleName))
			.collect(Collectors.toList());

		StringBuilder sb = new StringBuilder();
		sb.append("# -*- coding: utf-8 -*-\n\n");

		int idx = 0;
		for (int i = 0; i < all.size(); i += MAX_CLASS_ONE_FILE) {
			++idx;
			String rawName = TYPE_FILENAME_PREFIX.get(type) + idx;
			sb.append("from .").append(rawName).append(" import *\n");
			String filename = Paths.get(folder, rawName + ".py").toFile().getAbsolutePath();
			List <Class <?>> classes = all.subList(i, Math.min(i + MAX_CLASS_ONE_FILE, all.size()));
			writeSingleFile(filename, TYPE_IMPORT_LINES.get(type), genOpsLines(classes));
		}
		Files.write(Paths.get(folder, "__init__.py"),
			sb.toString().getBytes(StandardCharsets.UTF_8),
			StandardOpenOption.CREATE,
			StandardOpenOption.WRITE,
			StandardOpenOption.TRUNCATE_EXISTING);
	}

	static List <Class <?>> listSubtypeClasses(Class <?>... baseClasses) {
		return Arrays.stream(baseClasses)
			.flatMap(cls -> {
				try (ScanResult res = new ClassGraph().enableAllInfo().whitelistPackages("com.alibaba.alink").scan()) {
					return res.getSubclasses(cls.getName()).loadClasses().stream();
				}
			}).collect(Collectors.toList());
	}

	static boolean checkHasModel(String clsName) throws ClassNotFoundException {
		if (clsName.startsWith("com.alibaba.alink")) {
			Class <? extends StreamOperator> cls = (Class <? extends StreamOperator>) Class.forName(clsName);
			try {
				Constructor init = cls.getConstructor(BatchOperator.class, Params.class);
				return init != null;
			} catch (NoSuchMethodException e) {
				return false;
			}
		}
		throw new RuntimeException("invalid className: " + clsName);
	}

	/**
	 * @param args [stream/batch  outDirectory  100]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length > 2) {
			// how many classes in one python soruce file.
			MAX_CLASS_ONE_FILE = Integer.parseInt(args[2]);
			if (args[0].equalsIgnoreCase("stream")) {
				genTypeInfoFolder(args[1], Type.STREAM);
			} else if (args[0].equalsIgnoreCase("batch")) {
				genTypeInfoFolder(args[1], Type.BATCH);
			} else {
				genTypeInfoFolder(args[1], Type.PIPELINE);
			}
		} else {
			throw new RuntimeException("Missing MaxNumClassesOneFile");
		}
	}

	enum Type {
		BATCH,
		STREAM,
		PIPELINE
	}
}
