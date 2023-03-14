package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.JsonConverter;

import java.util.Iterator;

public class GraphUtils {

    public static DataSet<Edge <String, Double>> rowToEdges(DataSet <Row> inputEdgeData, boolean hasWeightCol, boolean isDirected) {
        return inputEdgeData.flatMap(new FlatMapFunction <Row, Edge <String, Double>>() {
                @Override
                public void flatMap(Row value, Collector <Edge <String, Double>> out) throws Exception {
                    double weight = 1.0;
                    if (hasWeightCol) {
                        weight = Double.valueOf(String.valueOf(value.getField(2)));
                    }
                    out.collect(new Edge <>(String.valueOf(value.getField(0)), String.valueOf(value.getField(1)), weight));
                    if (isDirected) {
                        out.collect(new Edge <>(String.valueOf(value.getField(1)),String.valueOf(value.getField(0)), weight));
                    }
                }
            });
    }

    public static DataSet <Tuple2 <String, Long>> graphNodeIdMapping(DataSet <Row> edge, int[] colIds,
                                                                     DataSet <Row> vertex, int pos) {
        DataSet <String> nodeDataSet = edge.flatMap(new FlatMapFunction <Row, String>() {
            @Override
            public void flatMap(Row value, Collector <String> out) throws Exception {
                for (int i = 0; i < colIds.length; i++) {
                    out.collect(JsonConverter.toJson(value.getField(colIds[i])));
                }
            }
        }).name("graphNodeIdMapping_flatmap_edge_nodes");
        if (null != vertex) {
            nodeDataSet = nodeDataSet.union(vertex.map(new MapFunction <Row, String>() {
                @Override
                public String map(Row value) throws Exception {
                    return JsonConverter.toJson(value.getField(pos));
                }
            })).name("graphNodeIdMapping_map_vertex_nodes");
        }
        return nodeDataSet.distinct().name("graphNodeIdMapping_distinct_nodes")
            .map(new RichMapFunction <String, Tuple2 <String, Long>>() {

                long cnt;
                int numTasks;
                int taskId;

                @Override
                public void open(Configuration parameters) throws Exception {
                    cnt = 0L;
                    numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
                    taskId = getRuntimeContext().getIndexOfThisSubtask();
                }

                @Override
                public Tuple2 <String, Long> map(String value) throws Exception {
                    return Tuple2.of(value, numTasks * (cnt++) + taskId);
                }
            })
            .name("build_node_mapping");
    }

    public static DataSet <Row> mapOriginalToId(DataSet <Row> original, DataSet <Tuple2 <String, Long>> dict,
                                                int[] colIds) {
        DataSet <Row> tmp = original;
        for (int i = 0; i < colIds.length; i++) {
            int colId = colIds[i];
            tmp = tmp.coGroup(dict).where(new KeySelector <Row, String>() {
                @Override
                public String getKey(Row value) throws Exception {
                    return JsonConverter.toJson(value.getField(colId));
                }
            }).equalTo(0).with(new CoGroupFunction <Row, Tuple2 <String, Long>, Row>() {
                @Override
                public void coGroup(Iterable <Row> first, Iterable <Tuple2 <String, Long>> second,
                                    Collector <Row> out) throws Exception {
                    Iterator <Tuple2 <String, Long>> iterator2 = second.iterator();
                    if (iterator2.hasNext()) {
                        long idx = iterator2.next().f1;
                        for (Row r : first) {
                            r.setField(colId, idx);
                            out.collect(r);
                        }
                    }
                }
            }).name("mapOriginToId_cogroup_index_" + colId);
        }
        return tmp;
    }

    public static DataSet <Row> mapIdToOriginal(DataSet <Row> original, DataSet <Tuple2 <String, Long>> dict,
                                                int[] colIds, TypeInformation type) {
        DataSet <Row> tmp = original;
        for (int i = 0; i < colIds.length; i++) {
            int colId = colIds[i];
            tmp = tmp.coGroup(dict).where(new KeySelector <Row, Long>() {
                @Override
                public Long getKey(Row value) throws Exception {
                    return (Long) value.getField(colId);
                }
            }).equalTo(1).with(new CoGroupFunction <Row, Tuple2 <String, Long>, Row>() {
                @Override
                public void coGroup(Iterable <Row> first, Iterable <Tuple2 <String, Long>> second,
                                    Collector <Row> out) throws Exception {
                    Iterator <Tuple2 <String, Long>> iterator2 = second.iterator();
                    if (iterator2.hasNext()) {
                        String label = iterator2.next().f0;
                        for (Row r : first) {
                            r.setField(colId, JsonConverter.fromJson(label, type.getTypeClass()));
                            out.collect(r);
                        }
                    }
                }
            }).name("mapIdToOrigin_cogroup_index_" + colId);
        }
        return tmp;
    }
}