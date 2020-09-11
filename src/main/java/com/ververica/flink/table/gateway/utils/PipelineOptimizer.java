package com.ververica.flink.table.gateway.utils;

import com.ververica.flink.table.gateway.context.ExecutionContext;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pipeline Optimizer.
 */
public class PipelineOptimizer {
    private static final Logger logger = LoggerFactory.getLogger(PipelineOptimizer.class);

    static final Pattern SOURCE_TABLE_PREFIX = Pattern.compile("^Source:\\s(TableSourceScan|HiveTableSource)(.*)$");
    static final Pattern LEGACY_HIVE_TABLE_SOURCE = Pattern.compile("^\\((.+)\\)\\sTablePath:\\s(.+?),.*$");
    static final Pattern TABLE_SOURCE_SCAN = Pattern.compile("^\\(table=\\[\\[(.+?)\\]\\].*\\)$");
    static final Pattern SINK_TABLE_PREFIX = Pattern.compile("^Sink:\\s(Sink)\\(table=\\[(.+?)\\].*\\)$");

    public static Pipeline optimize(ExecutionContext<?> ctx, Pipeline origin) {
        return optimizeTableSourceSinkParallelism(ctx, origin);
    }

    private static Pipeline optimizeTableSourceSinkParallelism(ExecutionContext<?> ctx, Pipeline pipeline) {

        if (pipeline instanceof StreamGraph) {
            StreamGraph streamGraph = (StreamGraph) pipeline;
            Collection<Integer> sourceIDs = streamGraph.getSourceIDs();
            for (Integer sourceId : sourceIDs) {
                optimizeSourceTableParallelism(ctx, streamGraph, sourceId);
            }

            Collection<Integer> sinkIDs = ((StreamGraph) pipeline).getSinkIDs();
            for (Integer sinkId : sinkIDs) {
                optimizeSinkTableParallelism(ctx, streamGraph, sinkId);
            }
            return streamGraph;
        } else {
            return pipeline;
        }
    }

    private static StreamGraph optimizeSourceTableParallelism(ExecutionContext<?> ctx, StreamGraph streamGraph, Integer sourceId) {
        StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
        Matcher m = SOURCE_TABLE_PREFIX.matcher(sourceNode.getOperatorName());
        if (m.matches()) {
            String qualifier = null;
            boolean legacy = false;
            // sourceNode is a matched sourceTable
            String type = m.group(1);
            String description = m.group(2);
            if (type.equals("TableSourceScan")) {
                m = TABLE_SOURCE_SCAN.matcher(description);
                if (m.matches()) {
                    qualifier = String.join(".", Stream.of(m.group(1).split(",")).map(String::trim).collect(Collectors.toList()).subList(0,3));
                }
            } else if (type.equals("HiveTableSource")) {
                m = LEGACY_HIVE_TABLE_SOURCE.matcher(description);
                if (m.matches()) {
                    qualifier = m.group(2);
                    legacy = true;
                }
            }
            // if found, and configured parallelism, override default parallelism
            if (qualifier != null) {
                String configuredTableParallelism = findUserDefinedTableParallelism(ctx, qualifier, TableType.SOURCE, legacy);
                if (configuredTableParallelism != null) {
                    Integer sourceTableParallelism = Integer.valueOf(configuredTableParallelism);
                    sourceNode.setParallelism(sourceTableParallelism);
                    return optimizeForwardDownStreamNodes(streamGraph, sourceNode, sourceTableParallelism);
                }
            } else {
                logger.warn(String.format("recognize table source type %s, but extract pattern not match for %s", type, description));
            }
        }
        return streamGraph;
    }

    private static String findUserDefinedTableParallelism(ExecutionContext<?> ctx, String qualifier, TableType type, boolean legacy) {
        Map<String, String> props = ctx.getEnvironment().getConfiguration().asMap();
        String value = null;
        String[] parts = qualifier.split("\\.");
        if (parts.length > 3) {
            logger.warn(String.format("unexpect qualifier %s", qualifier));
        } else if (parts.length == 3) {
            value = props.get(formatParallelismKey(qualifier, type));
            if (value == null && !parts[0].equals(ctx.getTableEnvironment().getCurrentCatalog())) {
                return null;
            }
            parts = new String[]{parts[1], parts[2]};
        }
        if (value == null && parts.length == 2) {
            String dbTableQualifier = String.format("%s.%s", parts[0], parts[1]);
            value = props.get(formatParallelismKey(dbTableQualifier, type));
            if (value == null) {
                if (legacy) {
                    value = getLegacyTableParallelism(ctx, props, dbTableQualifier);
                } else if (!parts[0].equals(ctx.getTableEnvironment().getCurrentDatabase())) {
                    return null;
                } else {
                    parts = new String[]{parts[1]};
                }
            }
        }
        if (value == null && parts.length == 1) {
            value = props.get(formatParallelismKey(parts[0], type));
            return value;
        }
        return value;
    }

    /*
    * for the reason legacy HiveTableSource does not provide catalog info, we try to discover user properties "*.database.table" pattern for hive catalog manner, and
    * assume to matched if only filtered one result
    * */
    private static String getLegacyTableParallelism(ExecutionContext<?> ctx, Map<String, String> props, String legacyQualifier) {
        List<String> legacyKeys = props.keySet().stream().filter(key -> {
            String[] parts = key.split("\\.");
            return parts.length == 6 && key.startsWith("table.") && key.endsWith(legacyQualifier + ".parallelism") && ctx.getCatalogs().get(parts[2]) instanceof HiveCatalog;
        }).collect(Collectors.toList());
        if (legacyKeys.size() == 1) {
            return props.get(legacyKeys.get(0));
        } else if (legacyKeys.size() > 1) {
            logger.warn(String.format("find more than 1 legacyKeys %s for qualifier %s, skip..", legacyKeys, legacyQualifier));
        }
        return null;
    }

    private static String formatParallelismKey(String tableIdentifier, TableType type) {
        return String.format("table.%s.%s.parallelism", type.equals(TableType.SOURCE) ? "source":"sink", tableIdentifier);
    }

    private static StreamGraph optimizeForwardDownStreamNodes(StreamGraph streamGraph, StreamNode sourceNode, Integer sourceTableParallelism) {
        // set Forward downstream StreamNodes
        List<StreamEdge> outEdges = sourceNode.getOutEdges();
        if (outEdges.size() > 0) {
            for (StreamEdge edge : outEdges) {
                if (edge.getPartitioner() instanceof ForwardPartitioner) {
                    StreamNode downstreamNode = streamGraph.getStreamNode(edge.getTargetId());
                    downstreamNode.setParallelism(sourceTableParallelism);
                    // set downstream parallelism recursively
                    return optimizeForwardDownStreamNodes(streamGraph, downstreamNode, sourceTableParallelism);
                }
            }
        }
        return streamGraph;
    }

    private static StreamGraph optimizeSinkTableParallelism(ExecutionContext<?> ctx, StreamGraph streamGraph, Integer sinkId) {
        StreamNode sinkNode = streamGraph.getStreamNode(sinkId);
        Matcher m = SINK_TABLE_PREFIX.matcher(sinkNode.getOperatorName());
        if (m.matches()) {
            String type = m.group(1);
            String qualifier = m.group(2);
            if (qualifier != null) {
                String configuredTableParallelism = findUserDefinedTableParallelism(ctx, qualifier, TableType.SINK, false); // current sink optimize not support legacy mode
                if (configuredTableParallelism != null) {
                    Integer sinkTableParallelism = Integer.valueOf(configuredTableParallelism);
                    sinkNode.setParallelism(sinkTableParallelism);
                    return optimizeForwardUpStreamNodes(streamGraph, sinkNode, sinkTableParallelism);
                }
            } else {
                logger.warn(String.format("recognize table sink type %s, but extract pattern not match for %s", type, sinkNode.getOperatorName()));
            }
        }
        return streamGraph;
    }

    private static StreamGraph optimizeForwardUpStreamNodes(StreamGraph streamGraph, StreamNode sinkNode, Integer sinkTableParallelism) {
        // set Forward upstream StreamNodes
        List<StreamEdge> inEdges = sinkNode.getInEdges();
        if (inEdges.size() > 0) {
            for (StreamEdge edge : inEdges) {
                if (edge.getPartitioner() instanceof ForwardPartitioner) {
                    StreamNode upstreamNode = streamGraph.getStreamNode(edge.getSourceId());
                    upstreamNode.setParallelism(sinkTableParallelism);
                    // set upstream parallelism recursively
                    return optimizeForwardUpStreamNodes(streamGraph, upstreamNode, sinkTableParallelism);
                }
            }
        }
        return streamGraph;
    }

    enum TableType {
        SOURCE,
        SINK
    }
}
