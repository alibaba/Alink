package com.alibaba.alink.common.dl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.dl.utils.DLTypeUtils;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.TypeConvertStreamOp;
import com.alibaba.alink.params.dataproc.HasTargetType.TargetType;
import com.alibaba.alink.params.dl.BaseDLParams;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for executing deep learning tasks (e.g., tensorflow, pytorch). Note that BaseDLStreamOp is the most
 * powerful op for executing dl-ops and the tasks could know each other. In other words, in this op,: (1) we can write
 * anything back to Alink (2) each task know ips and ports of others, that is we can do distributed training tasks.
 * Moreover, the dataset can only be accesses once by default, unless explicitly saved. Any number of outputs are
 * allowed from scripts, even no outputs.
 */
public abstract class BaseDLStreamOp<T extends BaseDLStreamOp <T>> extends StreamOperator <T>
	implements BaseDLParams <T> {


	public BaseDLStreamOp() {
		this(new Params());
	}

	public BaseDLStreamOp(Params params) {
		super(params);
	}

	/**
	 * subclasses must implement this method to override the following values:
	 * 		resPyFiles,
	 * 		mainScriptFileName,
	 * 		numPss,
	 * 		userScriptMainFileName
	 */
	abstract protected void initDLSystemParams();

	// all DL system-related files
	protected List <String> resPyFiles;
	// `mainScriptFileName` and `entryFuncName` are the main file and its entrypoint provided by our wrapping
	protected String mainScriptFileName;
	protected Integer numPss = null;
	protected String userScriptMainFileName;

	// entry function
	private static final String entryFuncName = "entry_func";
	// User's main script is renamed to `userScriptMainFileName`, and `main` is called

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		initDLSystemParams();
		StreamOperator <?> in = checkAndGetFirst(inputs);

		if (null != getSelectedCols()) {
			in = in.select(getSelectedCols());
		}
		in = DLTypeUtils.doubleColumnsToFloat(in);

		List <String> doubleColNames = new ArrayList <>();

		String[] colNames = in.getColNames();
		TypeInformation <?>[] colTypes = in.getColTypes();
		for (int i = 0; i < colTypes.length; i += 1) {
			if (colTypes[i].equals(Types.DOUBLE)) {
				doubleColNames.add(colNames[i]);
			}
		}

		if (doubleColNames.size() > 0) {
			TypeConvertStreamOp typeConvertStreamOp = new TypeConvertStreamOp()
				.setTargetType(TargetType.FLOAT)
				.setSelectedCols(doubleColNames.toArray(new String[0]));
			in = typeConvertStreamOp.linkFrom(in);
		}

		ExternalFilesConfig externalFilesConfig = getUserFiles()
			.addFilePaths(resPyFiles)
			.addRenameMap(getMainScriptFile(), userScriptMainFileName);

		DLLauncherStreamOp dlLauncherStreamOp = new DLLauncherStreamOp()
			.setOutputSchemaStr(getOutputSchemaStr())
			.setEntryFunc(entryFuncName)
			.setMainScriptFile(mainScriptFileName)
			.setUserFiles(externalFilesConfig)
			.setUserParams(getUserParams())
			.setNumWorkers(getNumWorkers())
			.setNumPSs(numPss)
			.setPythonEnv(getPythonEnv())
			.linkFrom(in);

		setOutputTable(dlLauncherStreamOp.getOutputTable());
		return (T) this;
	}
}
