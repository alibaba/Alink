package com.alibaba.alink.operator.common.feature.binning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Some support functions for Binning.
 */
public class BinningUtil {
    /**
     * PositiveInf Str.
     */
    private static String POSITIVE_INF = "+inf";

    /**
     * NegativeInf Str.
     */
    private static String NEGATIVE_INF = "-inf";

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
     * Comparator for Object in SingleBorder.
     */
    static BinningComparableComparator objectComparator = new BinningComparableComparator();

    public static Double keepGivenDecimal(Double d, int decimal){
        if(null == d){
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(d);

        return bigDecimal.setScale(decimal, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    public static class BinningComparableComparator implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return 1;
            } else if (o2 == null) {
                return -1;
            }

            Comparable c1 = (Comparable)o1;
            Comparable c2 = (Comparable)o2;

            try {
                return c1.compareTo(c2);
            } catch (Exception e) {
                Double d1 = ((Number)o1).doubleValue();
                Double d2 = ((Number)o2).doubleValue();
                return d1.compareTo(d2);
            }
        }
    }

    static ArrayList<String> singleBorderArrayToStr(ArrayList<BinTypes.SingleBorder> borders, BinTypes.ColType type) {
        Preconditions.checkNotNull(borders, "SingleBorder Array is NULL!");
        ArrayList res = new ArrayList();
        if (type.isNumeric) {
            Preconditions.checkState(borders.size() == 2,
                "Border to string error, size: %s", borders.size());
            StringBuilder builder = new StringBuilder();
            for (BinTypes.SingleBorder border : borders) {
                builder.append(singleBorderToStr(border, type)).append(",");
            }
            builder.delete(builder.length() - 1, builder.length());
            res.add(builder.toString());
        } else {
            for (BinTypes.SingleBorder border : borders) {
                res.add(singleBorderToStr(border, type));
            }
        }
        return res;
    }

    private static String singleBorderToStr(BinTypes.SingleBorder border, BinTypes.ColType type) {
        StringBuilder res = new StringBuilder();
        switch (border.symbol) {
            case LESS:
                res.append(valueToStr(border, type));
                res.append(")");
                break;
            case LESS_EQUAL:
                res.append(valueToStr(border, type));
                res.append("]");
                break;
            case GREATER:
                res.append("(");
                res.append(valueToStr(border, type));
                break;
            case GREATER_EQUAL:
                res.append("[");
                res.append(valueToStr(border, type));
                break;
            default:
                res.append(valueToStr(border, type));
                break;
        }

        return res.toString();
    }

    private static String valueToStr(BinTypes.SingleBorder border, BinTypes.ColType type) {
        if (border.infType == BinTypes.InfType.NEGATIVE_INF) {
            return NEGATIVE_INF;
        } else if (border.infType == BinTypes.InfType.POSITIVE_INF) {
            return POSITIVE_INF;
        } else {
            return type.objToStrFunc.apply(border.record);
        }
    }

    public static Bin.BaseBin findNumericBin(Object obj, FeatureBorder featureBorder, BinTypes.ColType colType) {
        int ret = BinningUtil.numericBinarySearch(obj, featureBorder.bin.normBins, colType);
        return ret >= 0 ? featureBorder.bin.normBins.get(ret) :
            (obj == null ? featureBorder.bin.nullBin : featureBorder.bin.elseBin);
    }

    static int compareRecordSingleBorder(Object obj, BinTypes.SingleBorder singleBorder) {
        Preconditions.checkNotNull(obj, "CompareSingleBorderRecord not support Null obj!");

        if (singleBorder.infType == BinTypes.InfType.NEGATIVE_INF) {
            return 1;
        } else if (singleBorder.infType == BinTypes.InfType.POSITIVE_INF) {
            return -1;
        }

        return objectComparator.compare(obj, singleBorder.record);
    }

    static boolean isInSingleBorder(Object obj, BinTypes.SingleBorder singleBorder) {
        return applyOp(singleBorder.symbol, compareRecordSingleBorder(obj, singleBorder));
    }

    private static boolean applyOp(BinTypes.Symbol symbol, int ret) {
        switch (symbol) {
            case EQUAL:
                if (ret == 0) {return true;}
                break;
            case LESS:
                if (ret == -1) {return true;}
                break;
            case LESS_EQUAL:
                if (ret == -1 || ret == 0) {return true;}
                break;
            case GREATER:
                if (ret == 1) {return true;}
                break;
            case GREATER_EQUAL:
                if (ret == 0 || ret == 1) {return true;}
                break;
            default:
                throw new RuntimeException("Unknown symbol!");
        }

        return false;
    }

    static int compareNumericRecordBin(Object obj, Bin.BaseBin bin) {
        Preconditions.checkNotNull(bin.borders, "Border Array is NULL!");
        checkNumericBin(bin);
        BinTypes.SingleBorder left = bin.borders.get(0);
        BinTypes.SingleBorder right = bin.borders.get(1);

        boolean applyRet = isInSingleBorder(obj, left);
        if (!applyRet) {
            return -1;
        }
        applyRet = isInSingleBorder(obj, right);
        return applyRet ? 0 : 1;
    }

    private static int numericBinarySearch(Object key, List<Bin.BaseBin> list, BinTypes.ColType colType) {
        if (null == list) {
            return -1;
        }
        checkNormBinArrays(list, colType);
        int low = 0;
        int high = list.size() - 1;
        int middle;

        if (compareNumericRecordBin(key, list.get(low)) == -1 || compareNumericRecordBin(key, list.get(high)) == 1) {
            return -1;
        }

        while (low <= high) {
            middle = (low + high) / 2;
            int ret = compareNumericRecordBin(key, list.get(middle));
            if (ret == -1) {
                high = middle - 1;
            } else if (ret == 1) {
                low = middle + 1;
            } else {
                return middle;
            }
        }

        return -1;
    }

    static void checkNormBinArray(List<Bin.BaseBin> bins, BinTypes.ColType colType) {
        for (Bin.BaseBin bin : bins) {
            if (colType.isNumeric) {
                checkNumericBin(bin);
            } else {
                checkDiscreteBin(bin);
            }
        }
    }

    static void sortBaseBinArray(List<Bin.BaseBin> bins, BinTypes.ColType colType) {
        bins.sort(new Comparator<Bin.BaseBin>() {
            @Override
            public int compare(Bin.BaseBin o1, Bin.BaseBin o2) {
                return compareBin(o1, o2, colType);
            }
        });
    }

    private static int compareSingleBorder(BinTypes.SingleBorder left, BinTypes.SingleBorder right) {
        if (left.infType == BinTypes.InfType.NEGATIVE_INF && right.infType == BinTypes.InfType.NEGATIVE_INF) {
            return 0;
        }
        if (left.infType == BinTypes.InfType.NEGATIVE_INF) {
            return -1;
        }
        if (right.infType == BinTypes.InfType.NEGATIVE_INF) {
            return 1;
        }
        if (left.infType == BinTypes.InfType.POSITIVE_INF && right.infType == BinTypes.InfType.POSITIVE_INF) {
            return 0;
        }
        if (left.infType == BinTypes.InfType.POSITIVE_INF) {
            return 1;
        }
        if (right.infType == BinTypes.InfType.POSITIVE_INF) {
            return -1;
        }

        return objectComparator.compare(left.record, right.record);
    }

    private static int compareNumericBin(Bin.BaseBin left, Bin.BaseBin right) {
        checkNumericBin(left);
        checkNumericBin(right);

        BinTypes.SingleBorder leftLeftSingleBorder = left.left();
        BinTypes.SingleBorder leftRightSingleBorder = left.right();

        BinTypes.SingleBorder rightLeftSingleBorder = right.left();
        BinTypes.SingleBorder rightRightSingleBorder = right.right();

        int ret = compareSingleBorder(leftLeftSingleBorder, rightRightSingleBorder);

        if (ret == 1) {
            return ret;
        }

        if (ret == 0) {
            ret = 1;
            return ret;
        }

        ret = compareSingleBorder(leftRightSingleBorder, rightLeftSingleBorder);
        if (ret == -1) {
            return ret;
        }

        if (ret == 0) {
            ret = -1;
            return ret;
        }

        throw new IllegalStateException("Interval border error. Borders are crossed");
    }

    private static int compareBin(Bin.BaseBin left, Bin.BaseBin right, BinTypes.ColType colType) {
        Preconditions.checkNotNull(left.borders, "Border Array is NULL!");
        Preconditions.checkNotNull(right.borders, "Border Array is NULL!");

        if (!colType.isNumeric) {
            return 0;
        } else {
            return compareNumericBin(left, right);
        }
    }

    static void checkNormBinArrayCrossAfterSort(List<Bin.BaseBin> bins, BinTypes.ColType colType) {
        if (null == bins || bins.size() == 0) {
            return;
        }
        Preconditions.checkNotNull(bins.get(0).borders, "Border Array is NULL!");
        for (int i = 1; i < bins.size(); i++) {
            Preconditions.checkNotNull(bins.get(i).borders, "Border Array is NULL!");
            if (colType.isNumeric) {
                Preconditions.checkState(compareBin(bins.get(i - 1), bins.get(i), colType) == -1,
                    "Interval border error. Borders are crossed.");
            }
        }
    }

    static boolean checkNormBinArrayContinuousAfterSort(List<Bin.BaseBin> bins, BinTypes.ColType colType) {
        if (!colType.isNumeric) {
            return false;
        }
        Preconditions.checkState(bins.size() >= 1, "Continuous Bin must have at least one bin: (-inf, +inf)");
        Bin.BaseBin bin = bins.get(0);
        Preconditions.checkState(bin.left().infType.equals(BinTypes.InfType.NEGATIVE_INF) && bin.left().symbol
                .equals(BinTypes.Symbol.GREATER),
            "The first border of a Continuous Bin must be (-inf, current is " + bin.left().infType + ", " + bin.left().symbol);
        boolean isLeftOpen = bin.right().symbol.equals(BinTypes.Symbol.LESS_EQUAL);
        Object pre = bin.right().record;
        for (int i = 1; i < bins.size(); i++) {
            bin = bins.get(i);
            Preconditions.checkState(bin.left().infType.equals(BinTypes.InfType.NOT_INF), "The interval border of a Continous Bin can not have inf value!");
            Preconditions.checkState(isLeftOpen == bin.left().symbol.equals(BinTypes.Symbol.GREATER),
                "The borders of a Continuous Bin must be all leftOpen or all rightOpen!");
            if(i < bins.size() - 1){
                Preconditions.checkState(isLeftOpen == bin.right().symbol.equals(BinTypes.Symbol.LESS_EQUAL),
                    "The borders of a Continuous Bin must be all leftOpen or all rightOpen!");
            }
            Preconditions.checkState(objectComparator.compare(pre, bin.left().record) == 0,
                "It's not a Continuous Bin, pre border is " + pre + "; current border is " + bin.left().record);
            pre = bin.right().record;
        }
        Preconditions.checkState(bin.right().infType.equals(BinTypes.InfType.POSITIVE_INF) && bin.right().symbol
                .equals(BinTypes.Symbol.LESS),
            "The last border of a Continuous Bin must be ,+inf)");
        return isLeftOpen;
    }

    static Tuple2<Number[], Boolean> borderToSplitsArray(List<Bin.BaseBin> bins, BinTypes.ColType colType) {
        Preconditions.checkArgument(colType.isNumeric, "borderToSplitsArray only support numeric type!");
        boolean isLeftOpen = BinningUtil.checkNormBinArrayContinuousAfterSort(bins, colType);
        Number[] splitsArray = new Number[bins.size() - 1];
        for(int i = 0; i < bins.size() - 1; i++){
            splitsArray[i] = (Number)bins.get(i).right().record;
        }
        return Tuple2.of(splitsArray, isLeftOpen);
    }

    public static List<Bin.BaseBin> splitsArrayToBorder(Number[] splits, boolean leftOpen) {
        List<Bin.BaseBin> list = new ArrayList<>();
        BinTypes.Symbol leftSymbol = leftOpen ? BinTypes.Symbol.GREATER : BinTypes.Symbol.GREATER_EQUAL;
        BinTypes.Symbol rightSymbol = leftOpen ? BinTypes.Symbol.LESS_EQUAL : BinTypes.Symbol.LESS;

        if (splits == null || splits.length == 0) {
            list.add(new Bin.BaseBin(0L,
                new BinTypes.SingleBorder(BinTypes.Symbol.GREATER, BinTypes.InfType.NEGATIVE_INF),
                new BinTypes.SingleBorder(BinTypes.Symbol.LESS, BinTypes.InfType.POSITIVE_INF)));
        } else {
            list.add(new Bin.BaseBin(0L,
                new BinTypes.SingleBorder(BinTypes.Symbol.GREATER, BinTypes.InfType.NEGATIVE_INF),
                new BinTypes.SingleBorder(rightSymbol, splits[0])));
            for (int i = 0; i < splits.length - 1; i++) {
                list.add(new Bin.BaseBin((long) i + 1L,
                    new BinTypes.SingleBorder(leftSymbol, splits[i]),
                    new BinTypes.SingleBorder(rightSymbol, splits[i + 1])));
            }
            list.add(new Bin.BaseBin((long) splits.length,
                new BinTypes.SingleBorder(leftSymbol, splits[splits.length - 1]),
                new BinTypes.SingleBorder(BinTypes.Symbol.LESS, BinTypes.InfType.POSITIVE_INF)));
        }
        return list;
    }

    public static Bin createNumericBin(Number[] splits, boolean leftOpen){
        Bin bin = new Bin();
        bin.normBins = BinningUtil.splitsArrayToBorder(splits, leftOpen);
        bin.nullBin = new Bin.BaseBin(nullIndex(bin.normBins.size()));
        return bin;
    }

    private static void checkNumericBin(Bin.BaseBin bin) {
        Preconditions.checkNotNull(bin.borders, "SingleBorder Array is NULL!");
        Preconditions.checkState(bin.borders.size() == 2,
            "NumericIntervalBorderError, size:%s", bin.borders.size());
    }

    private static void checkDiscreteBin(Bin.BaseBin bin) {
        Preconditions.checkNotNull(bin.values, "Border Array is NULL!");
        Preconditions.checkState(bin.values.size() > 0,
            "DiscreteIntervalValuesError, size:%s", bin.values.size());
    }

    private static void checkNormBinArrays(List<Bin.BaseBin> list, BinTypes.ColType colType) {
        if (null == list) {
            return;
        }
        Preconditions.checkState(list.size() > 0, "Norm Bin Array is empty!");
        for (Bin.BaseBin bin : list) {
            if (colType.isNumeric) {
                checkNumericBin(bin);
            } else {
                checkDiscreteBin(bin);
            }
        }
    }

    public static boolean isFeatureContinuous(FeatureBorder featureBorder) {
        if (!featureBorder.colType.isNumeric) {
            return false;
        }

        List<Bin.BaseBin> norm = featureBorder.bin.normBins;

        if (norm == null || norm.isEmpty()) {
            return false;
        }

        if (!norm.get(0).left().infType.equals(BinTypes.InfType.NEGATIVE_INF)) {
            return false;
        }

        if (!norm.get(norm.size() - 1).right().infType.equals(BinTypes.InfType.POSITIVE_INF)) {
            return false;
        }

        for (int i = 0; i < norm.size() - 1; ++i) {
            Bin.BaseBin bin = norm.get(i);
            Bin.BaseBin next = norm.get(i + 1);

            if (!bin.right().getRecord().equals(next.left().getRecord())) {
                return false;
            }

            if (next.left().symbol.equals(bin.right().symbol)) {
                return false;
            }

            if (bin.left().symbol.equals(bin.right().symbol)) {
                return false;
            }
        }

        return true;
    }

    public static long nullIndex(int normBinSize){
        return normBinSize;
    }

    public static long elseIndex(int normBinSize){
        return normBinSize + 1;
    }

    public static int getBinEncodeVectorSize(FeatureBorder featureBorder){
        return featureBorder.bin.normBins.size() + (featureBorder.colType.isNumeric ? NUMERIC_BIN_SIZE : DISCRETE_BIN_SIZE);
    }
}
