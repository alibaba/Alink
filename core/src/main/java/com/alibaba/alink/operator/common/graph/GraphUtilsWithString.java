package com.alibaba.alink.operator.common.graph;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.SortUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * The input vertices type could be double, string and long. In this class, we transform the vertices type to
 * long type, and graph algorithm run with long type vertices. In the output step, we transform the long type to
 * its original type.
 * <p>
 * for each graph, it just need to form a map of vertices.
 * and it need to get schema.
 * <p>
 * in this class, we finally need to transform the string to the origin type.
 * <p>
 * this class shouldn't be used outside.
 */

public class GraphUtilsWithString {
    /**
     * @param schema the columns which need to get transformed. it can be determined in vertex or edge column.
     * @param transformNum the number of the columns which need to get transformed.
     */

    // 0 if String, 1 if Double, 2 if Long.
    public TypeInformation type;
    //first transform input data into string, and then construct long map data.
    //when output, transform the long map data into string, and then transform to the out type.
    public DataSet<Tuple2<String, Long>> nodeMapping;

    public GraphUtilsWithString(DataSet<Row> edges, DataSet<Row> vertices, TypeInformation vertexType, boolean stable) {
        this.type = vertexType;
        if (!stable) {
            build(vertices, 1, vertexType);
            return;
        }
        buildStable(vertices, 1, vertexType);
    }

    /**
     * building node mapping.
     * from string/double/other to long
     * transform what type to string, and then transform string to long.
     */

    public GraphUtilsWithString(DataSet<Row> edges, TypeInformation vertexType) {
        build(edges, 2, vertexType);
    }


    public GraphUtilsWithString(DataSet<Row> edges, TypeInformation vertexType, boolean stable) {
        if (!stable) {
            build(edges, 2, vertexType);
            return;
        }
        this.type = vertexType;

        buildStable(edges, 2, vertexType);
    }

    private void build(DataSet<Row> in, int rowSize, TypeInformation vertexType) {
        this.type = vertexType;
        this.nodeMapping = in
            .mapPartition(new CollectVertices(rowSize))
            .distinct(0)
            .mapPartition(new RichMapPartitionFunction<Tuple1<String>, Tuple2<String, Long>>() {
                private static final long serialVersionUID = -7436140108787244483L;
                int cnt = 0;

                @Override
                public void mapPartition(Iterable<Tuple1<String>> values, Collector<Tuple2<String, Long>> out) {
                    int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
                    int taskId = getRuntimeContext().getIndexOfThisSubtask();
                    for (Tuple1<String> node : values) {
                        out.collect(Tuple2.of(node.f0, new Long(numTasks * cnt + taskId)));
                        cnt++;
                    }
                }
            }).name("build_node_mapping");
    }

    //in can be edges or vertices. if row size is 1, in is vertex table; if row size is 2, in is edge table.
    private void buildStable(DataSet<Row> in, int rowSize, TypeInformation vertexType) {
        DataSet<Row> nodes = in
            .mapPartition(new CollectRowVertices(rowSize))
            .distinct(new RowKeySelector(0));

        Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sortResult =
            SortUtils.pSort(nodes, 0);


        this.nodeMapping = sortResult.f0
            .partitionCustom(new Partitioner <Integer>() {
                private static final long serialVersionUID = 7033675545004935349L;

                @Override
                public int partition(Integer key, int numPartitions) {
                    return key;
                }
            }, 0)
            .mapPartition(new RichMapPartitionFunction <Tuple2<Integer, Row>, Tuple2<String, Long>>() {

                List<Tuple2 <Integer, Long>> sortInfo;
                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    sortInfo = getRuntimeContext().getBroadcastVariable("sortPartitionId");
                    sortInfo.sort(new Comparator <Tuple2 <Integer, Long>>() {
                        @Override
                        public int compare(Tuple2 <Integer, Long> o1, Tuple2 <Integer, Long> o2) {
                            return o1.f0.compareTo(o2.f0);
                        }
                    });
                    long cumSum = 0;
                    for (int i = 0; i < sortInfo.size(); i++) {
                        long temp = sortInfo.get(i).f1;
                        sortInfo.get(i).f1 = cumSum;
                        cumSum += temp;
                    }
                }

                @Override
                public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <String, Long>> out)
                    throws Exception {
                    int partitionId = -1;
                    List<String> strings = new ArrayList <>();
                    for (Tuple2 <Integer, Row> value : values) {
                        partitionId = value.f0;
                        strings.add((String) value.f1.getField(0));
                    }
                    if (partitionId == -1) {
                        return;
                    }
                    long start = sortInfo.get(partitionId).f1;
                    strings.sort(new Comparator <String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            return o1.compareTo(o2);
                        }
                    });
                    for (String string : strings) {
                        out.collect(Tuple2.of(string, start++));
                    }
                }
            })
            .withBroadcastSet(sortResult.f1, "sortPartitionId");

    }
    /**
     * Collect all vertices data.
     */
    public static class CollectRowVertices implements MapPartitionFunction<Row, Row> {
        private static final long serialVersionUID = -5370585830086767905L;

        int rowSize;

        CollectRowVertices(int rowSize) {
            this.rowSize = rowSize;
        }

        @Override
        public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
            Row res = new Row(1);
            for (Row value : values) {
                for (int i = 0; i < rowSize; ++i) {
                    res.setField(0, value.getField(i));
                    out.collect(res);
                }
            }
        }
    }

    public static class CollectVertices implements MapPartitionFunction<Row, Tuple1<String>> {
        private static final long serialVersionUID = -5370585830086767905L;
        int rowSize;

        CollectVertices(int rowSize) {
            this.rowSize = rowSize;
        }

        @Override
        public void mapPartition(Iterable<Row> values, Collector<Tuple1<String>> out) throws Exception {
            Tuple1<String> res = new Tuple1 <>();
            for (Row value : values) {
                for (int i = 0; i < rowSize; ++i) {
                    res.setFields((String) value.getField(i));
                    out.collect(res);
                }
            }
        }
    }

    public static DataSet<Row> input2json(BatchOperator in, String[] selectedCols,
                                          int transformSize, boolean needWeight) {
        boolean hasWeightCol = !(selectedCols.length == transformSize);
        return in.select(selectedCols).getDataSet()
            .mapPartition(new MapInputData2Json(transformSize, hasWeightCol, needWeight));
    }

    private static class MapInputData2Json implements MapPartitionFunction<Row, Row> {
        private int transformSize;
        private boolean hasWeightCol;
        private boolean needWeight;

        MapInputData2Json(int transformSize, boolean hasWeightCol, boolean needWeight) {
            this.transformSize = transformSize;
            this.hasWeightCol = hasWeightCol;
            this.needWeight = needWeight;
        }

        @Override
        public void mapPartition(Iterable<Row> values, Collector<Row> out) throws Exception {
            if (hasWeightCol || !needWeight) {
                for (Row value : values) {
                    for (int i = 0; i < transformSize; i++) {
                        value.setField(i, JsonConverter.toJson(value.getField(i)));
                    }
                    out.collect(value);
                }
            } else {
                for (Row value : values) {
                    Row res = new Row(transformSize + 1);
                    for (int i = 0; i < transformSize; i++) {
                        res.setField(i, JsonConverter.toJson(value.getField(i)));
                    }
                    res.setField(transformSize, 1.0);
                    out.collect(res);
                }
            }
        }
    }

    ////////////////Following are transform of input data.
    private static <IN, O> DataSet<O> mapInputOperation(DataSet<IN> inputData,
                                                        DataSet<Tuple2<String, Long>> nodeMapping,
                                                        TransformInputOperation func0,
                                                        TransformInputOperation func1,
                                                        TransformInputOperation func2) {
        int operationNum = 1 + (func1 == null ? 0 : 1) + (func2 == null ? 0 : 1);


        if (operationNum == 1) {
            return inCoGroupOperation(inputData, nodeMapping, func0, 0);
        }
        DataSet temp1 = inCoGroupOperation(inputData, nodeMapping, func0, 0);

        if (operationNum == 2) {
            return inCoGroupOperation(temp1, nodeMapping, func1, 1);
        }
        DataSet temp2 = inCoGroupOperation(temp1, nodeMapping, func1, 1);
        return inCoGroupOperation(temp2, nodeMapping, func2, 2);
    }

    private static <I, O> DataSet<O> inCoGroupOperation(DataSet<I> input,
                                                        DataSet<Tuple2<String, Long>> nodeMapping,
                                                        TransformInputOperation <I, O> func, int field) {

        String nameInfo = "transform_output_field" + field;
        CoGroupOperator.CoGroupOperatorSets<I, Tuple2<String, Long>>.CoGroupOperatorSetsPredicate temp;
        if (func.keySelector == null) {
            temp = input.coGroup(nodeMapping).where(field);
        } else {
            temp = input.coGroup(nodeMapping).where(func.keySelector);
        }

        return temp.equalTo(0)
            .with(new InCoGroupFunc<>(func))
            .name(nameInfo)
            .returns(func.getOClass());

    }

    private static class InCoGroupFunc<I, O> implements CoGroupFunction<I, Tuple2<String, Long>, O> {
        private static final long serialVersionUID = -5868579475777563997L;
        TransformInputOperation <I, O> func;

        InCoGroupFunc(TransformInputOperation <I, O> func) {
            this.func = func;
        }

        @Override
        public void coGroup(Iterable<I> first, Iterable<Tuple2<String, Long>> second, Collector<O> out) throws Exception {
            Long node = second.iterator().next().f1;
            func.transform(first, node, out);
        }
    }


    /**
     * This is the base interface defined for transform operation of each field.
     */
    private static abstract class TransformInputOperation<IN, O> implements Serializable {
        private static final long serialVersionUID = -5864865066998765568L;

        abstract void transform(Iterable<IN> iterableData, Long node, Collector<O> transformedData);

        TypeInformation type;

        TransformInputOperation(TypeInformation type) {
            this.type = type;
        }

        TypeInformation outType = null;

        TypeInformation getOClass() {

            return outType;
        }

        KeySelector keySelector = null;
    }


    /**
     * transform what type to long.(vertex)
     */
    public DataSet<Vertex<Long, Tuple3<Double, Double, Integer>>> inputType2longVertexCDC(DataSet<Row> inputData, boolean hasWeightCol) {
        return mapInputOperation(inputData, nodeMapping, new CDCInputOperation(type, hasWeightCol), null, null);
    }

    private static class CDCInputOperation
        extends TransformInputOperation <Row,
                Vertex<Long, Tuple3<Double, Double, Integer>>> {
        private static final long serialVersionUID = -1551136311194112775L;
        private boolean hasWeightCol;

        CDCInputOperation(TypeInformation type, boolean hasWeightCol) {
            super(type);
            outType = TypeInformation.of(new TypeHint<Vertex<Long, Tuple3<Double, Double, Integer>>>() {
            });
            this.hasWeightCol = hasWeightCol;
            keySelector = new RowKeySelector(0);
        }

        @Override
        void transform(Iterable<Row> iterableData,
                       Long node, Collector<Vertex<Long, Tuple3<Double, Double, Integer>>> transformedData) {
            for (Row data : iterableData) {
                Vertex<Long, Tuple3<Double, Double, Integer>> res = new Vertex<>();
                res.f0 = node;
                double weight = hasWeightCol ? (Double) data.getField(2) : 1.0;
                res.f1 = new Tuple3<>(Double.valueOf(data.getField(1).toString()), weight, 0);
                transformedData.collect(res);
            }
        }
    }

    private static class RowKeySelector implements KeySelector<Row, String> {
        private static final long serialVersionUID = 7514280642434354647L;
        int index;

        private RowKeySelector(int index) {
            this.index = index;
        }

        @Override
        public String getKey(Row value) {
            return (String) value.getField(index);
        }
    }

    /**
     * transform what type to long.(vertex) hasWeight seems always to be false.
     */
    public DataSet<Vertex<Long, Long>> transformInputVertexWithWeight(DataSet<Row> inputData) {
        return mapInputOperation(inputData, nodeMapping,
            new InputVertexWithWeightOperation (type), null, null);
    }

    private static class InputVertexWithWeightOperation
        extends TransformInputOperation <Row,
                Vertex<Long, Long>> {

        private static final long serialVersionUID = -5467940763129396379L;

        InputVertexWithWeightOperation(TypeInformation type) {
            super(type);
            this.outType = TypeInformation.of(new TypeHint<Vertex<Long, Long>>() {});
            this.keySelector = new RowKeySelector(0);
        }

        @Override
        void transform(Iterable<Row> iterableData, Long node, Collector<Vertex<Long, Long>> transformedData) {
            Vertex <Long, Long> res = new Vertex <>();
            for (Row data : iterableData) {
                res.f0 = node;
                res.f1 = (Long) data.getField(1);
                transformedData.collect(res);
            }
        }
    }

    public DataSet<Vertex<Long, Double>> transformInputVertexWithoutWeight(DataSet<Row> inputData) {
        return mapInputOperation(inputData, nodeMapping,
            new InputVertexWithoutWeightOperation (type), null, null);
    }

    private static class InputVertexWithoutWeightOperation
        extends TransformInputOperation <Row,
        Vertex<Long, Double>> {

        private static final long serialVersionUID = -5467940763129396379L;

        InputVertexWithoutWeightOperation(TypeInformation type) {
            super(type);
            this.outType = TypeInformation.of(new TypeHint<Vertex<Long, Double>>() {});
            this.keySelector = new RowKeySelector(0);
        }

        @Override
        void transform(Iterable<Row> iterableData, Long node, Collector<Vertex<Long, Double>> transformedData) {
            Vertex <Long, Double> res = new Vertex <>();
            for (Row data : iterableData) {
                res.f0 = node;
                res.f1 = Double.valueOf(node);
                transformedData.collect(res);
            }
        }
    }

    private static class InputRow2Tuple3Operation
        extends TransformInputOperation <Row, Tuple3<Long, String, Double>> {
        private static final long serialVersionUID = 4857770458983364478L;
        private boolean hasWeightCol;

        InputRow2Tuple3Operation(TypeInformation type, boolean hasWeightCol) {
            super(type);
            outType = TypeInformation.of(new TypeHint<Tuple3<Long, String, Double>>() {
            });
            this.hasWeightCol = hasWeightCol;
            keySelector = new RowKeySelector(0);
        }

        @Override
        void transform(Iterable<Row> iterableData, Long node, Collector<Tuple3<Long, String, Double>> transformedData) {
            for (Row row : iterableData) {
                double weight = hasWeightCol ? (double) row.getField(2) : 1.0;
                transformedData.collect(Tuple3.of(node, (String) row.getField(1), weight));
            }
        }
    }

    private static class InputMapSecondOperation
        extends TransformInputOperation <Tuple3<Long, String, Double>, Tuple3<Long, Long, Double>> {

        private static final long serialVersionUID = 7949557533955138510L;

        InputMapSecondOperation(TypeInformation type) {
            super(type);
            outType = TypeInformation.of(new TypeHint<Tuple3<Long, Long, Double>>() {});
        }

        @Override
        void transform(Iterable<Tuple3<Long, String, Double>> iterableData, Long node,
                       Collector<Tuple3<Long, Long, Double>> transformedData) {
            for (Tuple3<Long, String, Double> data : iterableData) {
                transformedData.collect(Tuple3.of(data.f0, node, data.f2));
            }
        }
    }

    private static class InputMap2EdgeOperation
        extends TransformInputOperation <Tuple3<Long, String, Double>, Edge<Long, Double>> {

        private static final long serialVersionUID = -1738639055886882855L;

        InputMap2EdgeOperation(TypeInformation type) {
            super(type);
            outType = TypeInformation.of(new TypeHint<Edge<Long, Double>>() {
            });
        }

        @Override
        void transform(Iterable<Tuple3<Long, String, Double>> iterableData, Long node, Collector<Edge<Long, Double>> transformedData) {
            for (Tuple3<Long, String, Double> data : iterableData) {
                transformedData.collect(new Edge<>(data.f0, node, data.f2));
            }
        }
    }

    /**
     * transform what type to long.(edge)
     * the input is row, and then transform and get it as Edge.
     * the input size is important, it determines the value of edge.
     */
    public DataSet<Tuple3<Long, Long, Double>> inputType2longTuple3(DataSet<Row> inputRow, Boolean haveWeight) {
        return mapInputOperation(inputRow, nodeMapping,
            new InputRow2Tuple3Operation(type, haveWeight), new InputMapSecondOperation(type), null);
    }

    public DataSet<Edge<Long, Double>> inputType2longEdge(DataSet<Row> inputRow, Boolean haveWeight) {
        return mapInputOperation(inputRow, nodeMapping,
            new InputRow2Tuple3Operation(type, haveWeight), new InputMap2EdgeOperation(type), null);
    }


    ////////////////Following are transform of output data.

    private static <IN> DataSet<Row> mapOutputOperation(DataSet<IN> inputData,
                                                        DataSet<Tuple2<String, Long>> nodeMapping,
                                                        OutTransformOperation func0,
                                                        OutTransformOperation func1,
                                                        OutTransformOperation func2) {
        int operationNum = 1 + (func1 == null ? 0 : 1) + (func2 == null ? 0 : 1);


        if (operationNum == 1) {
            return outCoGroupOperation(inputData, nodeMapping, func0, 0);
        }
        DataSet temp1 = outCoGroupOperation(inputData, nodeMapping, func0, 0);

        if (operationNum == 2) {
            return outCoGroupOperation(temp1, nodeMapping, func1, 1);
        }
        DataSet temp2 = outCoGroupOperation(temp1, nodeMapping, func1, 1);
        return outCoGroupOperation(temp2, nodeMapping, func2, 2);
    }

    private static <I, O> DataSet<O> outCoGroupOperation(DataSet<I> input,
                                                         DataSet<Tuple2<String, Long>> nodeMapping,
                                                         OutTransformOperation<I, O> func, int field) {
        String nameInfo = "transform_output_field" + field;
        return input
            .coGroup(nodeMapping).where(field).equalTo(1)
            .with(new OutCoGroupFunc<>(func))
            .name(nameInfo)
            .returns(func.getOClass());

    }

    private static class OutCoGroupFunc<I, O> implements CoGroupFunction<I, Tuple2<String, Long>, O> {
        private static final long serialVersionUID = -5868579475777563997L;
        OutTransformOperation<I, O> func;

        OutCoGroupFunc(OutTransformOperation<I, O> func) {
            this.func = func;
        }

        @Override
        public void coGroup(Iterable<I> first, Iterable<Tuple2<String, Long>> second, Collector<O> out) throws Exception {
            String node = second.iterator().next().f0;
            func.transform(first, node, out);
        }
    }


    /**
     * This is the base interface defined for transform operation of each field.
     */
    private static abstract class OutTransformOperation<IN, O> implements Serializable {
        private static final long serialVersionUID = -5864865066998765568L;

        abstract void transform(Iterable<IN> iterableData, String node, Collector<O> transformedData);

        TypeInformation type;

        OutTransformOperation(TypeInformation type) {
            this.type = type;
        }

        TypeInformation outType = null;

        TypeInformation getOClass() {

            return outType;
        }
    }

    private static class TransformFirstField<V> extends
        OutTransformOperation<Tuple3<Long, Long, V>, Tuple3<String, Long, V>> {

        private static final long serialVersionUID = 6160716516002826568L;

        TransformFirstField(TypeInformation type, TypeInformation identify) {
            super(type);
            outType = new TupleTypeInfo(Types.STRING, Types.LONG, identify);
        }

        @Override
        public void transform(Iterable<Tuple3<Long, Long, V>> iterableData, String node, Collector<Tuple3<String, Long, V>> transformedData) {
            for (Tuple3<Long, Long, V> e : iterableData) {
                Tuple3<String, Long, V> res = Tuple3.of(node, e.f1, e.f2);
                transformedData.collect(res);
            }
        }
    }

    private static class TransformSecondField<V> extends
        OutTransformOperation<Tuple3<String, Long, V>, Tuple3<String, String, V>> {

        private static final long serialVersionUID = -57491897739022180L;

        TransformSecondField(TypeInformation type, TypeInformation identify) {
            super(type);
            outType = new TupleTypeInfo(Types.STRING, Types.STRING, identify);
        }

        @Override
        public void transform(Iterable<Tuple3<String, Long, V>> iterableData, String node, Collector<Tuple3<String, String, V>> transformedData) {
            for (Tuple3<String, Long, V> e : iterableData) {
                transformedData.collect(Tuple3.of(e.f0, node, e.f2));
            }
        }
    }


    /**
     * transform KCore output.
     */
    public DataSet<Row> long2outputKCore(DataSet<Edge<Long, Double>> edge) {

        return mapOutputOperation(edge, nodeMapping,
            new KCoreOutFirstFieldOperation(this.type), new KCoreOutSecondFieldOperation(this.type), null);
    }

    private static class KCoreOutFirstFieldOperation extends
        OutTransformOperation<Edge<Long, Double>, Tuple3<String, Long, Double>> {

        private static final long serialVersionUID = 1487924540161367974L;

        KCoreOutFirstFieldOperation(TypeInformation type) {
            super(type);
            outType = new TupleTypeInfo(Types.STRING, Types.LONG, Types.DOUBLE);
        }

        @Override
        public void transform(Iterable<Edge<Long, Double>> iterableData, String node,
                              Collector<Tuple3<String, Long, Double>> transformedData) {
            for (Edge<Long, Double> e : iterableData) {
                Tuple3<String, Long, Double> res = Tuple3.of(node, e.f1, e.f2);
                transformedData.collect(res);
            }
        }
    }

    private static class KCoreOutSecondFieldOperation
        extends OutTransformOperation<Tuple3<String, Long, Double>, Row> {

        private static final long serialVersionUID = -2569140678923424283L;

        KCoreOutSecondFieldOperation(TypeInformation type) {
            super(type);
            outType = new RowTypeInfo(type, Types.STRING);
        }

        @Override
        public void transform(Iterable<Tuple3<String, Long, Double>> iterableData, String node, Collector<Row> transformedData) {
            for (Tuple3<String, Long, Double> value : iterableData) {
                Row res = new Row(2);
                res.setField(0, JsonConverter.fromJson(value.f0, type.getTypeClass()));
                res.setField(1, JsonConverter.fromJson(node, type.getTypeClass()));
                transformedData.collect(res);
            }
        }
    }


    /**
     * transform line algorithm output.
     */
    public DataSet<Row> mapLine(DataSet<Tuple2<Long, double[]>> solution) {
        return mapOutputOperation(solution, nodeMapping, new LineOutOperation(this.type), null, null);
    }


    private static class LineOutOperation
        extends OutTransformOperation<Tuple2<Long, double[]>, Row> {

        private static final long serialVersionUID = 8239130805757955107L;

        LineOutOperation(TypeInformation type) {
            super(type);
            outType = new RowTypeInfo(type, TypeInformation.of(DenseVector.class));
        }

        @Override
        public void transform(Iterable<Tuple2<Long, double[]>> first, String node, Collector<Row> out) {
            for (Tuple2<Long, double[]> e : first) {
                Row res = new Row(2);
                res.setField(0, JsonConverter.fromJson(node, type.getTypeClass()));
                DenseVector dv = new DenseVector(e.f1);
                //normalize
                dv.normalizeEqual(2);
                res.setField(1, dv);
                out.collect(res);
            }
        }
    }

    /**
     * transform TreeDepth output.
     */
    public DataSet<Row> long2outputTreeDepth(DataSet<Tuple3<Long, Long, Double>> edge) {
        return mapOutputOperation(edge, nodeMapping,
            new TransformFirstField(this.type, Types.DOUBLE), new TransformTreeDepthOut(type), null);
    }

    private static class TransformTreeDepthOut extends
        OutTransformOperation<Tuple3<String, Long, Comparable>, Row> {

        TransformTreeDepthOut(TypeInformation type) {
            super(type);
            outType = new RowTypeInfo(type, type, Types.LONG);
        }

        @Override
        public void transform(Iterable<Tuple3<String, Long, Comparable>> iterableData, String node, Collector<Row> transformedData) {
            for (Tuple3<String, Long, Comparable> value : iterableData) {
                Row r = new Row(3);
                r.setField(0, JsonConverter.fromJson(value.f0, type.getTypeClass()));
                r.setField(1, JsonConverter.fromJson(node, type.getTypeClass()));
                r.setField(2, value.f2);
                transformedData.collect(r);
            }
        }
    }


    /**
     * transform TriangleList output.
     */
    public DataSet<Row> long2outputTriangleList(DataSet<Tuple3<Long, Long, Long>> edge) {
        return mapOutputOperation(edge, nodeMapping, new TransformFirstField(this.type, Types.LONG),
            new TransformSecondField(this.type, Types.LONG), new TriangleListOutOperation(this.type));
    }

    private static class TriangleListOutOperation
        extends OutTransformOperation<Tuple3<String, String, Long>, Row> {

        private static final long serialVersionUID = -1582550106348170178L;

        TriangleListOutOperation(TypeInformation type) {
            super(type);
            outType = new RowTypeInfo(type, type, type);
        }

        @Override
        void transform(Iterable<Tuple3<String, String, Long>> iterableData, String node, Collector<Row> transformedData) {
            for (Tuple3<String, String, Long> value : iterableData) {
                Row res = new Row(3);
                res.setField(0, JsonConverter.fromJson(value.f0, type.getTypeClass()));
                res.setField(1, JsonConverter.fromJson(value.f1, type.getTypeClass()));
                res.setField(2, JsonConverter.fromJson(node, type.getTypeClass()));
                transformedData.collect(res);
            }
        }
    }

    /**
     * transform CommunityDetectionClassify, CommunityDetectionCluster,
     * connectedComponent and SingleSourceShortestPath.
     */
    public DataSet<Row> double2outputTypeVertex(DataSet<Vertex<Long, Double>> vertex, TypeInformation valueType) {
        return mapOutputOperation(vertex, nodeMapping, new VertexOutOperation(this.type, valueType),
            null, null);
    }

    private static class VertexOutOperation
        extends OutTransformOperation<Vertex<Long, Double>, Row> {
        private static final long serialVersionUID = 7861957948865947534L;
        TypeInformation valueType;

        VertexOutOperation(TypeInformation type, TypeInformation valueType) {
            super(type);
            this.valueType = valueType;
            outType = new RowTypeInfo(type, valueType);
        }

        @Override
        void transform(Iterable<Vertex<Long, Double>> iterableData, String node, Collector<Row> transformedData) {
            if (valueType.getTypeClass() == Double.class) {
                for (Vertex<Long, Double> value : iterableData) {
                    Row res = new Row(2);
                    res.setField(0, JsonConverter.fromJson(node, type.getTypeClass()));
                    res.setField(1, value.f1);
                    transformedData.collect(res);
                }
            } else {
                for (Vertex<Long, Double> value : iterableData) {
                    Row res = new Row(2);
                    res.setField(0, JsonConverter.fromJson(node, type.getTypeClass()));
                    res.setField(1, value.f1.longValue());
                    transformedData.collect(res);
                }
            }
        }
    }


    /**
     * transform VCC output.
     */
    public DataSet<Row> long2outputVCC(DataSet<Tuple4<Long, Long, Long, Double>> value) {
        return mapOutputOperation(value, nodeMapping, new VCCOutOperation(this.type), null, null);
    }

    private static class VCCOutOperation
        extends OutTransformOperation<Tuple4<Long, Long, Long, Double>, Row> {

        private static final long serialVersionUID = 5229375281455715888L;

        VCCOutOperation(TypeInformation type) {
            super(type);
            outType = new RowTypeInfo(type, Types.LONG, Types.LONG, Types.DOUBLE);
        }

        @Override
        void transform(Iterable<Tuple4<Long, Long, Long, Double>> iterableData, String node, Collector<Row> transformedData) {
            for (Tuple4<Long, Long, Long, Double> value : iterableData) {
                Row res = new Row(4);
                res.setField(1, value.f1);
                res.setField(2, value.f2);
                res.setField(3, value.f3);
                res.setField(0, JsonConverter.fromJson(node, type.getTypeClass()));
                transformedData.collect(res);
            }
        }
    }

    /**
     * transform long to string.(ecc)
     */
    public DataSet<Row> long2outputTypeECC(DataSet<Tuple6<Long, Long, Long, Long, Long, Double>> value) {
        return mapOutputOperation(value, nodeMapping, new ECCFirstOperation(type), new ECCOutOperation(type), null);
    }

    private static class ECCFirstOperation
        extends OutTransformOperation<Tuple6<Long, Long, Long, Long, Long, Double>,
        Tuple6<String, Long, Long, Long, Long, Double>> {

        private static final long serialVersionUID = 2520258591521524937L;

        ECCFirstOperation(TypeInformation type) {
            super(type);
            outType = new TupleTypeInfo(Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.DOUBLE);
        }

        @Override
        void transform(Iterable<Tuple6<Long, Long, Long, Long, Long, Double>> iterableData, String node,
                       Collector<Tuple6<String, Long, Long, Long, Long, Double>> transformedData) {
            for (Tuple6<Long, Long, Long, Long, Long, Double> e : iterableData) {
                transformedData.collect(Tuple6.of(node, e.f1, e.f2, e.f3, e.f4, e.f5));
            }
        }
    }

    private static class ECCOutOperation
        extends OutTransformOperation<Tuple6<String, Long, Long, Long, Long, Double>, Row> {

        private static final long serialVersionUID = 6756130760384222623L;

        ECCOutOperation(TypeInformation type) {
            super(type);
            outType = new RowTypeInfo(type, type, Types.LONG, Types.LONG, Types.LONG, Types.DOUBLE);
        }

        @Override
        void transform(Iterable<Tuple6<String, Long, Long, Long, Long, Double>> iterableData,
                       String node, Collector<Row> transformedData) {
            for (Tuple6<String, Long, Long, Long, Long, Double> value : iterableData) {
                Row res = new Row(6);
                res.setField(2, value.f2);
                res.setField(3, value.f3);
                res.setField(4, value.f4);
                res.setField(5, value.f5);
                res.setField(0, JsonConverter.fromJson(value.f0, type.getTypeClass()));
                res.setField(1, JsonConverter.fromJson(node, type.getTypeClass()));
                transformedData.collect(res);
            }
        }
    }


    public DataSet<Tuple1<Long>> string2longSource(String source, long sessionId, TypeInformation<?> vertexType) {
        Row rowData = new Row(1);
        if (Types.BIG_INT.equals(vertexType) || Types.BIG_DEC.equals(vertexType) || Types.LONG.equals(vertexType)) {
            rowData.setField(0, Double.valueOf(source).longValue());
        } else if (Types.INT.equals(vertexType)) {
            rowData.setField(0, Double.valueOf(source).intValue());
        } else if (Types.FLOAT.equals(vertexType)) {
            rowData.setField(0, Double.valueOf(source).floatValue());
        } else if (Types.SHORT.equals(vertexType)) {
            rowData.setField(0, Double.valueOf(source).shortValue());
        } else if (Types.STRING.equals(vertexType)) {
            rowData.setField(0, source);
        } else {
            rowData.setField(0, JsonConverter.fromJson(source, type.getTypeClass()));
        }
        Row[] sourceRow = new Row[]{rowData};
        DataSet<Row> dataSet = MLEnvironmentFactory.get(sessionId)
            .getExecutionEnvironment().fromCollection(Arrays.asList(sourceRow));
        DataSet<Tuple1<String>> jsonedSource = dataSet
            .map(new MapFunction<Row, Tuple1<String>>() {
                @Override
                public Tuple1<String> map(Row value) throws Exception {
                    return Tuple1.of(JsonConverter.toJson(value.getField(0)));
                }
            });

        return jsonedSource
            .join(nodeMapping)
            .where(0)
            .equalTo(0)
            .with(new JoinFunction<Tuple1<String>, Tuple2<String, Long>, Tuple1<Long>>() {
                private static final long serialVersionUID = 7809017413078937725L;

                @Override
                public Tuple1<Long> join(Tuple1<String> first, Tuple2<String, Long> second) throws Exception {
                    return Tuple1.of(second.f1);
                }
            });
    }

    /**
     * transform long to string.(SingleSourceShortestPath)
     */
    public DataSet<Row> long2StringSSSP(DataSet<Tuple4<Long, Long, Long, Double>> input) {

        DataSet<Tuple2 <Object, Tuple4<Long, Long, Long, Double>>> stage1 =
            input.map(new MapFunction <Tuple4 <Long, Long, Long, Double>, Tuple2 <Long, Tuple4<Long, Long, Long, Double>>>() {
                @Override
                public Tuple2 <Long, Tuple4<Long, Long, Long, Double>> map(Tuple4 <Long, Long, Long, Double> value)
                    throws Exception {
                    return Tuple2.of(value.f0, value);
                }
            }).leftOuterJoin(nodeMapping).where(0).equalTo(1).with(new Long2StringJoinFunction<>(type));

        DataSet<Tuple2 <Object, Tuple4<Object, Long, Long, Double>>> stage2 =
            stage1.map(
                new MapFunction <Tuple2 <Object, Tuple4 <Long, Long, Long, Double>>, Tuple2 <Long, Tuple4 <Object, Long, Long, Double>>>() {
                    @Override
                    public Tuple2 <Long, Tuple4 <Object, Long, Long, Double>> map(
                        Tuple2 <Object, Tuple4 <Long, Long, Long, Double>> value) throws Exception {
                        return Tuple2.of(value.f1.f1, Tuple4.of(value.f0, value.f1.f1, value.f1.f2, value.f1.f3));
                    }
                })
                .leftOuterJoin(nodeMapping).where(0).equalTo(1).with(new Long2StringJoinFunction <>(type));

        DataSet<Tuple2 <Object, Tuple4 <Object, Object, Long, Double>>> stage3 = stage2.map(
            new MapFunction <Tuple2 <Object, Tuple4<Object, Long, Long, Double>>, Tuple2 <Long, Tuple4 <Object, Object, Long, Double>>>() {

                @Override
                public Tuple2 <Long, Tuple4 <Object, Object, Long, Double>> map(
                    Tuple2 <Object, Tuple4<Object, Long, Long, Double>> value) throws Exception {
                    return Tuple2.of(value.f1.f2, Tuple4.of(value.f1.f0, value.f0, value.f1.f2, value.f1.f3));
                }
            })
            .leftOuterJoin(nodeMapping).where(0).equalTo(1).with(new Long2StringJoinFunction<>(type));

        return stage3.map(
            new MapFunction <Tuple2 <Object, Tuple4 <Object, Object, Long, Double>>, Row>() {
                @Override
                public Row map(Tuple2 <Object, Tuple4 <Object, Object, Long, Double>> value) throws Exception {
                    Row row = new Row(4);
                    row.setField(0, value.f1.f0);
                    row.setField(1, value.f1.f1);
                    row.setField(2, value.f0);
                    row.setField(3, value.f1.f3);
                    return row;
                }
            });
    }

    private static class Long2StringJoinFunction<VALUE>
        implements JoinFunction <Tuple2<Long, VALUE>, Tuple2 <String, Long>, Tuple2<Object, VALUE>> {
        private static final long serialVersionUID = 3788131720307372418L;

        TypeInformation type;

        Long2StringJoinFunction(TypeInformation type) {
            this.type = type;
        }
        @Override
        public Tuple2<Object, VALUE> join(Tuple2<Long, VALUE> row, Tuple2 <String, Long> stringLongTuple2) throws Exception {
            if (stringLongTuple2 == null) {
                return Tuple2.of(null, row.f1);
            }
            return Tuple2.of(JsonConverter.fromJson(stringLongTuple2.f0, this.type.getTypeClass()), row.f1);
        }
    }
}