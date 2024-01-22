package com.alibaba.alink.operator.local.feature;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.dataproc.MultiStringIndexerModelDataConverter;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.dataproc.HasSelectedColTypes;
import com.alibaba.alink.params.feature.CrossFeatureTrainParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Cross selected columns to build new vector type data.
 */
@InputPorts(values = {@PortSpec(PortType.DATA)})
@OutputPorts(values = {@PortSpec(value = PortType.MODEL)})
@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("Cross特征训练")
@NameEn("Cross Feature Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.feature.CrossFeature")
public class CrossFeatureTrainLocalOp extends LocalOperator <CrossFeatureTrainLocalOp>
	implements CrossFeatureTrainParams <CrossFeatureTrainLocalOp> {

	public CrossFeatureTrainLocalOp() {
		super(new Params());
	}

	public CrossFeatureTrainLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator in = checkAndGetFirst(inputs);
		String[] selectedCols = getSelectedCols();

		final String[] selectedColType = new String[selectedCols.length];
		for (int i = 0; i < selectedCols.length; i++) {
			selectedColType[i] = FlinkTypeConverter.getTypeString(
				TableUtil.findColTypeWithAssertAndHint(in.getSchema(), selectedCols[i]));
		}

		MTable mt = in.getOutputTable();
		List <Tuple3 <Integer, String, Long>> indexedToken = new ArrayList <>();
		for (int k = 0; k < selectedCols.length; k++) {
			int colIndex = TableUtil.findColIndexWithAssert(in.getSchema(), selectedCols[k]);
			boolean containNull = false;
			TreeSet <String> tokens = new TreeSet <>();
			for (Row row : mt.getRows()) {
				Object obj = row.getField(colIndex);
				if (null == obj) {
					containNull = true;
				} else {
					tokens.add(obj.toString());
				}
			}
			long curIndex = 0L;
			for (String token : tokens) {
				indexedToken.add(Tuple3.of(k, token, curIndex));
				curIndex++;
			}
			if (containNull) {
				indexedToken.add(Tuple3.of(k, null, curIndex));
			}
		}


		Params meta = new Params()
			.set(HasSelectedCols.SELECTED_COLS, selectedCols)
			.set(HasSelectedColTypes.SELECTED_COL_TYPES, selectedColType);
		RowCollector collector = new RowCollector();
		new MultiStringIndexerModelDataConverter().save(Tuple2.of(meta, indexedToken), collector);

		this.setOutputTable(
			new MTable(collector.getRows(), new MultiStringIndexerModelDataConverter().getModelSchema()));

	}

}
