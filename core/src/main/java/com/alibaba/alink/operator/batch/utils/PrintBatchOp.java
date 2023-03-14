package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.ExceptionWithErrorCode;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.BaseSinkBatchOp;

import java.io.PrintStream;
import java.util.List;

/**
 * Print batch op to std out.
 */
@IoOpAnnotation(name = "print", ioType = IOType.SinkBatch)
@NameCn("批式数据打印")
@NameEn("Print Operation")
public class PrintBatchOp extends BaseSinkBatchOp <PrintBatchOp> {

	private static final long serialVersionUID = -8361687806231696283L;
	private static PrintStream batchPrintStream = System.out;

	public PrintBatchOp() {
		this(null);
	}

	public PrintBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(PrintBatchOp.class), params);
	}

	public static void setBatchPrintStream(PrintStream printStream) {
		batchPrintStream = printStream;
	}

	@Override
	protected PrintBatchOp sinkFrom(BatchOperator <?> in) {
		if (null != in.getOutputTable()) {
			try {
				List <Row> rows = in.collect();
				batchPrintStream.println(TableUtil.formatTitle(in.getColNames()));
				for (Row row : rows) {
					batchPrintStream.println(TableUtil.formatRows(row));
				}
			} catch (ExceptionWithErrorCode ex) {
				throw ex;
			} catch (Exception ex) {
				throw new AkUnclassifiedErrorException("Error. ", ex);
			}
		}
		return this;
	}

}
