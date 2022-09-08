package com.alibaba.alink.operator.local.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.params.io.LibSvmSourceParams;

import java.util.List;

/**
 * A data source that reads libsvm format data.
 */
@NameCn("LibSvm文件读入")
public final class LibSvmSourceLocalOp extends BaseSourceLocalOp <LibSvmSourceLocalOp>
	implements LibSvmSourceParams <LibSvmSourceLocalOp> {

	public LibSvmSourceLocalOp() {
		this(new Params());
	}

	public LibSvmSourceLocalOp(Params params) {
		super(params);
	}

	@Override
	public MTable initializeDataSource() {
		List <Row> rows = new CsvSourceLocalOp()
			.setFilePath(getFilePath())
			.setFieldDelimiter("\n")
			.setSchemaStr("content string")
			.setPartitions(getPartitions())
			.getOutputTable()
			.getRows();

		final int startIndex = getParams().get(LibSvmSourceParams.START_INDEX);

		for (int i = 0; i < rows.size(); i++) {
			String line = (String) rows.get(i).getField(0);
			Tuple2 <Double, Vector> labelAndFeatures = parseLibSvmFormat(line, startIndex);
			rows.set(i, Row.of(labelAndFeatures.f0, labelAndFeatures.f1));
		}

		return new MTable(rows, LIB_SVM_TABLE_SCHEMA);
	}

	public static Tuple2 <Double, Vector> parseLibSvmFormat(String line, int startIndex) {
		if (StringUtils.isNullOrWhitespaceOnly(line)) {
			return Tuple2.of(null, null);
		}
		int firstSpacePos = line.indexOf(' ');
		if (firstSpacePos < 0) {
			return Tuple2.of(Double.valueOf(line), VectorUtil.getVector(""));
		}
		String labelStr = line.substring(0, firstSpacePos);
		String featuresStr = line.substring(firstSpacePos + 1);
		Vector featuresVec = VectorUtil.getVector(featuresStr);
		if (featuresVec instanceof SparseVector) {
			int[] indices = ((SparseVector) featuresVec).getIndices();
			for (int i = 0; i < indices.length; i++) {
				indices[i] = indices[i] - startIndex;
			}
		}
		return Tuple2.of(Double.valueOf(labelStr), featuresVec);
	}

	public static final TableSchema LIB_SVM_TABLE_SCHEMA = new TableSchema(new String[] {"label", "features"},
		new TypeInformation[] {Types.DOUBLE(), AlinkTypes.VECTOR});

}
