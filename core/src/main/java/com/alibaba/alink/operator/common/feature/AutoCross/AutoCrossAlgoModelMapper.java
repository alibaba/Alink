package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;
import com.alibaba.alink.params.feature.featuregenerator.HasAppendOriginalData;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class AutoCrossAlgoModelMapper extends ModelMapper {
	private static final long serialVersionUID = -4500389710522943248L;
	private String[] dataCols;
	private int[] numericalIndices;
	private int vecIndex = -2;
	private OneHotOperator operator;
	private int[] cumsumIndex;
	private final AutoCrossPredictParams.OutputFormat outputFormat;
	TriFunction<Tuple4<SlicedSelectedSample, Integer, int[], int[]>, OneHotOperator, SlicedResult, Row> mapOperator;

	boolean appendOriginalVec = true;

	public static class FeatureSet implements Serializable {
		private static final long serialVersionUID = 3402906686076385472L;
		public int numRawFeatures;
		public String[] numericalCols;
		public String vecColName;
		public List <int[]> crossFeatureSet;
		public List <Double> scores;
		public int[] indexSize;
		public boolean hasDiscrete;
	}

	//todo may support input vector col.
	public AutoCrossAlgoModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(modelSchema, dataSchema, params);
		outputFormat = params.get(AutoCrossPredictParams.OUTPUT_FORMAT);
		appendOriginalVec = params.get(HasAppendOriginalData.APPEND_ORIGINAL_DATA);
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																						   TableSchema dataSchema,
																						   Params params) {
		dataCols = dataSchema.getFieldNames();
		String[] outputCols = new String[] {params.get(AutoCrossPredictParams.OUTPUT_COL)};
		TypeInformation[] outputTypes = new TypeInformation[] {AlinkTypes.VECTOR};
		return Tuple4.of(dataCols, outputCols, outputTypes, params.get(HasReservedColsDefaultAsNull.RESERVED_COLS));

	}

	@Override
	public void loadModel(List <Row> modelRows) {
		String jsonStr = modelRows.stream().filter(row -> row.getField(0).equals(0L))
			.map(row -> (String) row.getField(1))
			.collect(Collectors.toList()).get(0);
		FeatureSet fs = JsonConverter.fromJson(jsonStr, FeatureSet.class);
		numericalIndices = TableUtil.findColIndices(dataCols, fs.numericalCols);
		if (vecIndex == -2) {
			vecIndex = TableUtil.findColIndex(dataCols, fs.vecColName);
		}
		int crossFeatureSetSize = fs.crossFeatureSet.size();
		operator = new OneHotOperator(fs.numRawFeatures, fs.crossFeatureSet, fs.indexSize);

		if (outputFormat == AutoCrossPredictParams.OutputFormat.Dense) {
			cumsumIndex = new int[fs.indexSize.length+fs.crossFeatureSet.size()-1];
			cumsumIndex[0] = fs.indexSize[0];
			for (int i = 1; i < fs.indexSize.length; i++) {
				cumsumIndex[i] = cumsumIndex[i-1]+fs.indexSize[i];
			}
			for (int i = 0; i < fs.crossFeatureSet.size() - 1; i++) {
				int tempSize = 1;
				for (int v : fs.crossFeatureSet.get(i)) {
					tempSize*=v;
				}
				cumsumIndex[fs.indexSize.length+i] = cumsumIndex[fs.indexSize.length+i-1]+tempSize;
			}
		}
		if (outputFormat == AutoCrossPredictParams.OutputFormat.Sparse) {
			if (appendOriginalVec) {
				mapOperator = AutoCrossAlgoModelMapper::mapSparse;
			} else {
				mapOperator = AutoCrossAlgoModelMapper::mapSparseWithoutOriginal;
			}
		} else if (outputFormat == AutoCrossPredictParams.OutputFormat.Dense) {
			if (appendOriginalVec) {
				mapOperator = AutoCrossAlgoModelMapper::mapDense;
			} else {
				mapOperator = AutoCrossAlgoModelMapper::mapDenseWithoutOriginal;
			}
		}
		//inputBufferThreadLocal = ThreadLocal.withInitial(() -> new Row(ioSchema.f0.length));
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		//Row row = inputBufferThreadLocal.get();
		mapOperator.apply(Tuple4.of(selection, vecIndex, cumsumIndex, numericalIndices), operator, result);
	}


	private static Row mapSparseWithoutOriginal(Tuple4<SlicedSelectedSample, Integer, int[], int[]> rowAndIndex, OneHotOperator operator, SlicedResult result) {
		SlicedSelectedSample row = rowAndIndex.f0;
		int vecIndex = rowAndIndex.f1;
		SparseVector inputData = VectorUtil.getSparseVector(row.get(vecIndex));
		int inputDataSize = inputData.size();
		int inputDataNum = inputData.numberOfValues();
		SparseVector crossedVec = operator.oneHotData(inputData);
		int resSize = crossedVec.size() - inputDataSize;
		int resDataNum = crossedVec.numberOfValues() - inputDataNum;
		int[] resIndices = new int[resDataNum];
		double[] resValues = new double[resDataNum];
		System.arraycopy(crossedVec.getIndices(), inputDataNum, resIndices, 0, resDataNum);
		System.arraycopy(crossedVec.getValues(), inputDataNum, resValues, 0, resDataNum);
		for (int i = 0; i < resDataNum; i++) {
			resIndices[i] -= inputDataSize;
		}

		result.set(0, new SparseVector(resSize, resIndices, resValues));
		return null;
	}

	private static Row mapSparse(Tuple4<SlicedSelectedSample, Integer, int[], int[]> rowAndIndex, OneHotOperator operator, SlicedResult result) {
		SlicedSelectedSample row = rowAndIndex.f0;
		int vecIndex = rowAndIndex.f1;
		int[] numericalIndices = rowAndIndex.f3;
		SparseVector inVec = operator.oneHotData(VectorUtil.getSparseVector(row.get(vecIndex)));
		int svSize = inVec.size() + numericalIndices.length;
		int[] svIndices = new int[inVec.getIndices().length + numericalIndices.length];
		double[] svValues = new double[inVec.getIndices().length + numericalIndices.length];

		for (int i = 0; i < numericalIndices.length; i++) {
			svIndices[i] = i;
			svValues[i] = ((Number) row.get(numericalIndices[i])).doubleValue();
		}
		for (int i = 0; i < inVec.getIndices().length; i++) {
			inVec.getIndices()[i] += numericalIndices.length;
		}
		System.arraycopy(inVec.getIndices(), 0, svIndices, numericalIndices.length, inVec.getIndices().length);
		System.arraycopy(inVec.getValues(), 0, svValues, numericalIndices.length, inVec.getValues().length);
		result.set(0, new SparseVector(svSize, svIndices, svValues));
		return null;
	}

	private static Row mapDenseWithoutOriginal(Tuple4<SlicedSelectedSample, Integer, int[], int[]> rowAndIndex, OneHotOperator operator, SlicedResult result) {
		SlicedSelectedSample row = rowAndIndex.f0;
		int vecIndex = rowAndIndex.f1;
		SparseVector inputData = (SparseVector) VectorUtil.getVector(row.get(vecIndex));
		int inputDataSize = inputData.size();
		int inputDataNum = inputData.numberOfValues();
		SparseVector crossedVec = operator.oneHotData(inputData);
		int[] indices = inputData.getIndices();
		int resDataNum = crossedVec.numberOfValues() - inputDataNum;
		double[] resData = new double[resDataNum];
		for (int i = 0; i < resDataNum; i++) {
			resData[i] = indices[i + inputDataNum] - inputDataSize;
		}
		result.set(0, new DenseVector(resData));
		return null;
	}

	private static Row mapDense(Tuple4<SlicedSelectedSample, Integer, int[], int[]> rowAndIndex, OneHotOperator operator, SlicedResult result) {
		SlicedSelectedSample row = rowAndIndex.f0;
		int vecIndex = rowAndIndex.f1;
		//int[] cumsumIndex = rowAndIndex.f2;
		int[] numericalIndices = rowAndIndex.f3;
		SparseVector inVec = (SparseVector) VectorUtil.getVector(row.get(vecIndex));
		inVec = operator.oneHotData(inVec);
		int[] indices = inVec.getIndices();
		//for (int i = 0; i < cumsumIndex.length; i++) {
		//	indices[i+1] -= cumsumIndex[i];
		//}
		double[] dIndices = new double[numericalIndices.length + indices.length];
		for (int i = 0; i < numericalIndices.length; i++) {
			dIndices[i] = ((Number) row.get(numericalIndices[i])).doubleValue();
		}
		for (int i = 0; i < indices.length; i++) {
			dIndices[i + numericalIndices.length] = indices[i];
		}
		result.set(0, new DenseVector(dIndices));
		return null;
	}

}
