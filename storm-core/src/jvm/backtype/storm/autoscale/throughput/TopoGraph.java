package backtype.storm.autoscale.throughput;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;

public class TopoGraph {

	class GraphNode {

		CompData compData;
		List<GraphNode> kids = new LinkedList<>();

		public GraphNode(CompData compData) {
			this.compData = compData;
		}

	}

	Map<String, GraphNode> rootNodes = new HashMap<>();
	Map<String, GraphNode> allNodes = new HashMap<>();

	public TopoGraph(PersistentData data) {

		// lets build the all nodes first, and spouts are roots
		for (Entry<String, BoltData> d : data.getBolts().entrySet()) {
			String key = d.getKey();
			GraphNode graphNode = new GraphNode(d.getValue());
			allNodes.put(key, graphNode);
		}
		for (Entry<String, SpoutData> d : data.getSpouts().entrySet()) {
			String key = d.getKey();
			GraphNode graphNode = new GraphNode(d.getValue());
			allNodes.put(key, graphNode);
			rootNodes.put(key, graphNode);
		}

		// link up kids
		for (GraphNode gn : allNodes.values()) {
			// only bolts are kids
			if (gn.compData instanceof BoltData) {
				for (ExecutorSummary es : gn.compData.getSummaries()) {
					for (GlobalStreamId gsid : es.get_stats().get_specific().get_bolt().get_execute_ms_avg()
							.get(PersistentData.metricTimeWindowSecsStr).keySet()) {
						allNodes.get(gsid.get_componentId()).kids.add(gn);
					}
				}
			}
		}
	}

	public Iterable<CompData> bredthFirstIterable() {
		Set<CompData> rv = new LinkedHashSet<>();
		Set<GraphNode> toprocess = new HashSet<>(rootNodes.values());
		do {
			Set<GraphNode> nextprocess = new HashSet<>();
			for (GraphNode gn : toprocess) {
				rv.add(gn.compData);
				nextprocess.addAll(gn.kids);
			}
			toprocess = nextprocess;
		} while (toprocess.size() > 0);
		return rv;
	}
}
