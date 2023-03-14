package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.HBaseOutputFormat;
import com.alibaba.alink.params.io.HBaseSinkParams;

/**
 * BatchOperator to sink data to HBase.
 */
@IoOpAnnotation(name = "hbase_batch_sink", ioType = IOType.SinkBatch)
@ParamSelectColumnSpec(name = "rowKeyCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("HBase导出")
@NameEn("HBase Sink")
public final class HBaseSinkBatchOp extends BaseSinkBatchOp <HBaseSinkBatchOp>
	implements HBaseSinkParams <HBaseSinkBatchOp> {

	public HBaseSinkBatchOp() {
		this(new Params());
	}

	public HBaseSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(HBaseSinkBatchOp.class), params);
	}

	@Override
	public HBaseSinkBatchOp sinkFrom(BatchOperator <?> in) {
		TableSchema schema = in.getSchema();

		in.getDataSet().output(
			new HBaseOutputFormat(getParams(), schema.getFieldNames(), schema.getFieldTypes())
		);

		return this;
	}

}
