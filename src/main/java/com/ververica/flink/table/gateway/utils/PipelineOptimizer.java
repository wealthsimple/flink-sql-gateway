package com.ververica.flink.table.gateway.utils;

import com.ververica.flink.table.gateway.context.ExecutionContext;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pipeline Optimizer.
 */
public class PipelineOptimizer {
	public static Pipeline optimize(ExecutionContext ctx, Pipeline origin) {
		return optimizeTableSourceSinkParallelism(ctx, origin);
	}

	private static Pipeline optimizeTableSourceSinkParallelism(ExecutionContext ctx, Pipeline pipeline) {

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

	private static StreamGraph optimizeSourceTableParallelism(ExecutionContext ctx, StreamGraph streamGraph, Integer sourceId) {
		StreamNode sourceNode = streamGraph.getStreamNode(sourceId);

		Integer sourceTableParallelism = null;
		String pattern = "Source: (\\w+)TableSource\\((.+)\\)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(sourceNode.getOperatorName());
		if (m.matches()) {
			// sourceNode is a matched sourceTable
			String type = m.group(1);
			String schema = m.group(2);
			String matchedTableName = findMatchedTable(ctx.getTableEnvironment(), type, schema);
			// if found, and configured parallelism, override default parallelism
			if (matchedTableName != null) {
				String configuredTableParallelism = ctx.getEnvironment().getConfiguration().asMap().get("table.source." + matchedTableName + ".parallelism");
				if (configuredTableParallelism != null) {
					sourceTableParallelism = Integer.valueOf(configuredTableParallelism);
					sourceNode.setParallelism(sourceTableParallelism);
					return optimizeForwardDownStreamNodes(streamGraph, sourceNode, sourceTableParallelism);
				}
			}
		}
		return streamGraph;
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

	private static StreamGraph optimizeSinkTableParallelism(ExecutionContext ctx, StreamGraph streamGraph, Integer sinkId) {
		StreamNode sinkNode = streamGraph.getStreamNode(sinkId);

		Integer sinkTableParallelism = null;
		String pattern = "Sink: (\\w+)TableSink\\((.+)\\)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(sinkNode.getOperatorName());
		if (m.matches()) {
			String type = m.group(1);
			String schema = m.group(2);
			String matchedTableName = findMatchedTable(ctx.getTableEnvironment(), type, schema);
			if (matchedTableName != null) {
				String configuredTableParallelism = ctx.getEnvironment().getConfiguration().asMap().get("table.sink." + matchedTableName + ".parallelism");
				if (configuredTableParallelism != null) {
					sinkTableParallelism = Integer.valueOf(configuredTableParallelism);
					sinkNode.setParallelism(sinkTableParallelism);
					return optimizeForwardUpStreamNodes(streamGraph, sinkNode, sinkTableParallelism);
				}
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

	private static String findMatchedTable(TableEnvironment tableEnv, String type, String schema) {
		String matchedTableName = null;
		String[] tables = tableEnv.listTables();
		for (String tablename : tables) {
			try {
				CatalogBaseTable table = tableEnv.getCatalog(tableEnv.getCurrentCatalog()).get().getTable(new ObjectPath(tableEnv.getCurrentDatabase(), tablename));

				Map<String, String> tableOptions = table.getOptions();
				String connectorType = Optional.of(tableOptions.get(ConnectorDescriptorValidator.CONNECTOR))
						.orElse(tableOptions.get(ConnectorDescriptorValidator.CONNECTOR_TYPE));
				String schemaStr = String.join(", ", table.getSchema().getFieldNames());
				if (connectorType.equalsIgnoreCase(type) && schemaStr.startsWith(schema)) {
					matchedTableName = tablename;
					break;
				}
			} catch (TableNotExistException e) {
			}

		}
		return matchedTableName;
	}
}
