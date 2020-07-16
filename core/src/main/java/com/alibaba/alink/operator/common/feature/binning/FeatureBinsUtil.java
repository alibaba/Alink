package com.alibaba.alink.operator.common.feature.binning;

import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.operator.common.feature.BinningModelDataConverter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/**
 * Some support functions for Binning.
 */
public class FeatureBinsUtil {
    /**
     * nullBin and elseBin.
     */
    static int DISCRETE_BIN_SIZE = 2;

    /**
     * nullBin.
     */
    static int NUMERIC_BIN_SIZE = 1;

    /**
     * Label for nullBin.
     */
    public static String NULL_LABEL = "NULL";

    /**
     * Label for elseBin.
     */
    public static String ELSE_LABEL = "ELSE";

    /**
     * String showed in web.
     */
    private static String WEB_STRING = "STRING";

    /**
     * String used in the sql.
     */
    private static String SQL_STRING = "VARCHAR";

    private static String POSITIVE_INF = "+inf";
    private static String NEGATIVE_INF = "-inf";
    private static String LEFT_OPEN = "(";
    private static String LEFT_CLOSE = "[";
    private static String RIGHT_OPEN = ")";
    private static String RIGHT_CLOSE = "]";
    private static String JOIN_DELIMITER = ",";

    public static BinTypes.ColType featureTypeToColType(String featureType, BinDivideType binDivideType) {
        return binDivideType.equals(BinDivideType.BUCKET) ? BinTypes.ColType.FLOAT : BinTypes.ColType.valueOf(
            getFlinkType(featureType));
    }

    public static String getTypeString(TypeInformation<?> type) {
        String typeStr = FlinkTypeConverter.getTypeString(type);
        if (SQL_STRING.equals(typeStr)) {
            typeStr = WEB_STRING;
        }
        return typeStr;
    }

    public static TypeInformation<?> getFlinkType(String typeStr) {
        if (WEB_STRING.equals(typeStr)) {
            typeStr = SQL_STRING;
        }
        return FlinkTypeConverter.getFlinkType(typeStr);
    }

    /**
     * Keep the given decimal of the number.
     */
    public static Double keepGivenDecimal(Double d, int decimal) {
        if (null == d) {
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(d);

        return bigDecimal.setScale(decimal, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * Keep the given decimal of the number.
     */
    public static Number keepGivenDecimal(Number d, int decimal) {
        if (null == d) {
            return null;
        }
        BigDecimal bigDecimal;
        if(d instanceof Double || d instanceof Float){
            bigDecimal = new BigDecimal(d.doubleValue());
            return bigDecimal.setScale(decimal, BigDecimal.ROUND_HALF_UP).doubleValue();
        }else{
            bigDecimal = new BigDecimal(d.longValue());
            return bigDecimal.setScale(decimal, BigDecimal.ROUND_HALF_UP).longValue();
        }
    }

    static Tuple2<Bins, Number[]> createNumericBin(Number[] splitsArray) {
        TreeSet<Number> set = new TreeSet<>(FeatureBinsUtil.numberComparator);
        Collections.addAll(set, splitsArray);
        splitsArray = set.toArray(new Number[0]);
        Bins bin = new Bins();
        for (int i = 0; i <= splitsArray.length; i++) {
            bin.normBins.add(new Bins.BaseBin((long)i));
        }
        bin.nullBin = new Bins.BaseBin(nullIndex(bin.normBins.size()));
        return Tuple2.of(bin, splitsArray);
    }

    public static DataSet<FeatureBinsCalculator> parseFeatureBinsModel(DataSet<Row> modelData) {
        return modelData.mapPartition(new MapPartitionFunction<Row, FeatureBinsCalculator>() {

            @Override
            public void mapPartition(Iterable<Row> values, Collector<FeatureBinsCalculator> out) throws Exception {
                List<Row> list = new ArrayList<>();
                values.forEach(list::add);
                new BinningModelDataConverter().load(list).forEach(out::collect);
            }
        });
    }

    public static int compareNumbers(Number o1, Number o2) {
        if (o1 instanceof Comparable && o2 instanceof Comparable
            && o1.getClass() == o2.getClass()) {
            return ((Comparable) o1).compareTo(o2);
        } else {
            return Double.compare(o1.doubleValue(), o2.doubleValue());
        }
    }

    public static FeatureBinsCalculator[] deSerialize(String str) {
        FeatureBins[] featureBins = FeatureBins.deSerialize(str);
        FeatureBinsCalculator[] binsBuilder = new FeatureBinsCalculator[featureBins.length];
        for (int i = 0; i < featureBins.length; i++) {
            binsBuilder[i] = FeatureBinsCalculatorTransformer.fromFeatureBins(featureBins[i]);
        }
        return binsBuilder;
    }

    public static String serialize(FeatureBinsCalculator... featureBinsCalculators) {
        FeatureBins[] featureBins = new FeatureBins[featureBinsCalculators.length];
        for (int i = 0; i < featureBins.length; i++) {
            featureBins[i] = FeatureBinsCalculatorTransformer.toFeatureBins(featureBinsCalculators[i]);
        }
        return FeatureBins.serialize(featureBins);
    }

    public static double calcWoe(long binTotal, long binPositiveTotal, long positiveTotal, long negativeTotal){
        long binNegativeTotal = binTotal - binPositiveTotal;
        Double woe = Double.NaN;
        if(positiveTotal > 0 && negativeTotal > 0){
            double binPositiveRate = 1.0 * (binPositiveTotal + 0.5) / positiveTotal;
            double binNegativeRate = 1.0 * (binNegativeTotal + 0.5) / negativeTotal;
            woe = Math.log(binPositiveRate / binNegativeRate);
        }
        return woe;
    }

    interface SerializableComparator<T> extends java.util.Comparator<T>, Serializable {}

    private static SerializableComparator<Number> numberComparator =
        (SerializableComparator<Number>) FeatureBinsUtil::compareNumbers;

    public static long nullIndex(int normBinSize) {
        return normBinSize;
    }

    public static long elseIndex(int normBinSize) {
        return normBinSize + 1;
    }

    public static int getBinEncodeVectorSize(FeatureBinsCalculator featureBinsCalculator) {
        return featureBinsCalculator.bin.normBins.size() + (featureBinsCalculator.isNumeric() ? NUMERIC_BIN_SIZE
            : DISCRETE_BIN_SIZE);
    }

    public static String[] cutsArrayToInterval(Number[] splitsArray, boolean leftOpen){
        if (splitsArray.length == 0) {
            return new String[]{
                LEFT_OPEN + NEGATIVE_INF + JOIN_DELIMITER + POSITIVE_INF + RIGHT_OPEN};
        }
        int size = splitsArray.length;
        String[] interval = new String[splitsArray.length + 1];
        String leftSymbol = (leftOpen ? LEFT_OPEN : LEFT_CLOSE);
        String rightSymbol = (leftOpen ? RIGHT_CLOSE : RIGHT_OPEN);
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                interval[i] = LEFT_OPEN + NEGATIVE_INF + JOIN_DELIMITER + keepGivenDecimal(splitsArray[i], 3).toString()
                    + rightSymbol;
            } else {
                interval[i] = leftSymbol + keepGivenDecimal(splitsArray[i - 1], 3).toString() + JOIN_DELIMITER
                    + keepGivenDecimal(splitsArray[i], 3).toString() + rightSymbol;
            }
        }
        interval[size] = leftSymbol + keepGivenDecimal(splitsArray[size - 1], 3).toString() + JOIN_DELIMITER + POSITIVE_INF
            + RIGHT_OPEN;
        return interval;
    }
}
