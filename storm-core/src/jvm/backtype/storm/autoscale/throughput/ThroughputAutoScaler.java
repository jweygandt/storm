package backtype.storm.autoscale.throughput;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.map.LazyMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.autoscale.IAutoScaler;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.BoltStats;
import backtype.storm.generated.BounceOptions;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GetInfoOptions;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.NumErrorsChoice;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.SpoutStats;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;
import clojure.lang.IPersistentMap;

/**
 * An auto-scale algorithm designed to manage the topology primarily by
 * capacity/throughput metrics.
 *
 */
public class ThroughputAutoScaler implements IAutoScaler {

	private static class PersistentData implements Serializable {
		Map<Object, Double> parallelism;

		private static final long serialVersionUID = 1L;

		public synchronized Map<Object, Double> getParallelism() {
			Map<Object, Double> p = new HashMap<Object, Double>();
			if (parallelism != null) {
				p.putAll(parallelism);
			}
			return p;
		}

		public synchronized void setParallelism(Map<Object, Double> parallelism) {
			this.parallelism = parallelism;
		}
	}

	private static int bouncecnt = 0;

	private final class MonitorThread extends Thread {

		private MonitorThread(String arg0) {
			super(arg0);
		}

		@Override
		public void run() {
			try {
				while (!done) {
					try {

						// Note: one may access the member variables of
						// ThroughputAutoScaler without synchronization.
						// However!! When accessing "PersistentData data" -
						// synchronize on data!

						try {
							// get from config

							Thread.sleep(10000);
							Map<?, ?> metrics = getMetrics();
							Map<Object, Double> parallelism = calculateParallelism(metrics);
							data.setParallelism(parallelism);
						} catch (Exception e) {
							continue;
						}

						if (bouncecnt++ == 0) {
							BounceOptions options = new BounceOptions();
							options.set_step1_wait_secs(1);
							options.set_step2_wait_secs(10);
							_nimbus.bounce(_stormName, options);
						}

						writePersistentData();

						try {
							TopologyInfo tinfo = _nimbus.getTopologyInfo(_stormId);
							LOG.info(tinfo.toString());
							LOG.info(new ArrayList<>(tinfo.get_executors()).toString());

							String tconf = _nimbus.getTopologyConf(_stormId);
							LOG.info(tconf.toString());

						} catch (NotAliveException e) {
							LOG.error("Topology died without a proper kill of the autoscaler, autoscaler terminating",
									e);
							break;
						}

					} catch (Exception e) {
						LOG.error("Something recoverable happend for autoscaler for topo=" + _stormName + " : "
								+ e.getLocalizedMessage(), e);
					}
				}
			} catch (Throwable t) {
				LOG.error("The autoscaler for: " + _stormName + " has failed, system must exit: "
						+ t.getLocalizedMessage());
				System.exit(1); // fail fast.
			}
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(ThroughputAutoScaler.class);

	// The following block of variables are somewhat like "finals"
	// They are set prior to the autoscaler thread starting, so they are
	// available to the Thread, properly. All other method's use of these
	// should properly synchronize.
	// When the thread finishes, _stormId remains set, and "stale", till the
	// next time the thread is started, when it is modified, again before
	// the start.
	// Generally most of the methods should either be very short running, simply
	// to manage the thread, or in the case of "real" activity (like configure)
	// will simply take the time they need. The main body of the thread will
	// likely
	// take some time, but it runs, for the most part, unsynchronized.
	// Of course the contents of PersistentData will need proper
	// synchronization. Please synchronize on PersistentData.
	private File _datadir;
	private Iface _nimbus;
	private String _stormName;
	private String _stormId;
	private PersistentData data;

	private volatile boolean done;
	private MonitorThread autoscaler = null;
	private long targetTps;

	@Override
	public synchronized void reLoadAndReStartAutoScaler(Iface nimbus, File datadir, String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf) {
		_datadir = datadir;
		_nimbus = nimbus;
		_stormName = stormName;
		_stormId = stormId;
		done = false;
		this.targetTps = (Long) totalStormConf.get(Config.TOPOLOGY_AUTO_SCALER_TARGET_THROUGHPUT);
		readPersistentData(true);

		autoscaler = new MonitorThread(this.getClass().getSimpleName() + "-" + stormName);
		autoscaler.start();
	}

	@Override
	public synchronized void stopAutoScaler() {
		if (autoscaler != null) {
			done = true;
			autoscaler.interrupt();
			autoscaler = null;
		}
	}

	@Override
	public boolean isRunning() {
		return !done;
	}

	private static int callcnt = 1;

	@Override
	public Object[] modifyConfigAndTopology(String stormId, @SuppressWarnings("rawtypes") Map stormConf,
			StormTopology topology) {

		// Make a mutable copy
		topology = topology.deepCopy();

		// Note! When you access data in "PersistentData data" please
		// synchronize on "data"

		int newval = callcnt++;

		Map<Object, Double> parallelism = data.getParallelism();

		LOG.info("Spouts:");
		for (Entry<String, SpoutSpec> ent : topology.get_spouts().entrySet()) {

			SpoutSpec spec = ent.getValue();
			String name = ent.getKey();

			if (name.equals("spout1")) {
				// spec.get_common().set_parallelism_hint(newval);
				// spec.get_common().set_json_conf("{\"topology.tasks\" : " +
				// newval + "}");
			}

			LOG.info("  " + name + " " + spec.get_spout_object().getSetField());
			ComponentCommon cc = spec.get_common();
			LOG.info("    " + "Parallelism : " + cc.get_parallelism_hint());
			LOG.info("    " + "Inputs");
			for (Entry<GlobalStreamId, Grouping> ient : cc.get_inputs().entrySet()) {
				LOG.info("      " + ient.getKey() + " : " + ient.getValue());
			}
			LOG.info("    " + "Streams");
			for (Entry<String, StreamInfo> sent : cc.get_streams().entrySet()) {
				LOG.info("      " + sent.getKey() + " : " + sent.getValue());
			}
			LOG.info("    " + "JSON Config : " + cc.get_json_conf());
		}

		LOG.info("Bolts:");
		for (Entry<String, Bolt> ent : topology.get_bolts().entrySet()) {
			Bolt spec = ent.getValue();
			String name = ent.getKey();

			if (name.equals("bolt1")) {
				// spec.get_common().set_parallelism_hint(newval);
				// spec.get_common().set_json_conf("{\"topology.tasks\" : " +
				// newval + "}");
			}

			LOG.info("  " + name + " " + spec.get_bolt_object().getSetField());
			ComponentCommon cc = spec.get_common();

			Double sf = parallelism.get(ent.getKey());
			if (sf == null || sf == 0d) {
				sf = 1d;
			}

			int parallelismHint = (int) Math.ceil(sf.doubleValue() * cc.get_parallelism_hint());
			System.out.println("Using parallelism hint of " + parallelismHint + " for " + ent.getKey());
			cc.set_parallelism_hint(parallelismHint);

			LOG.info("    " + "Parallelism : " + parallelismHint);
			LOG.info("    " + "Inputs");
			for (Entry<GlobalStreamId, Grouping> ient : cc.get_inputs().entrySet()) {
				LOG.info("      " + ient.getKey() + " : " + ient.getValue());
			}
			LOG.info("    " + "Streams");
			for (Entry<String, StreamInfo> sent : cc.get_streams().entrySet()) {
				LOG.info("      " + sent.getKey() + " : " + sent.getValue());
			}
			LOG.info("    " + "JSON Config : " + cc.get_json_conf());
		}

		LOG.info("State Spouts:");
		for (Entry<String, StateSpoutSpec> ent : topology.get_state_spouts().entrySet()) {
			StateSpoutSpec spec = ent.getValue();
			LOG.info("  " + ent.getKey() + " " + spec.get_state_spout_object().getSetField());
			ComponentCommon cc = spec.get_common();
			LOG.info("    " + "Parallelism : " + cc.get_parallelism_hint());
			LOG.info("    " + "Inputs");
			for (Entry<GlobalStreamId, Grouping> ient : cc.get_inputs().entrySet()) {
				LOG.info("      " + ient.getKey() + " : " + ient.getValue());
			}
			LOG.info("    " + "Streams");
			for (Entry<String, StreamInfo> sent : cc.get_streams().entrySet()) {
				LOG.info("      " + sent.getKey() + " : " + sent.getValue());
			}
			LOG.info("    " + "JSON Config : " + cc.get_json_conf());
		}

		IPersistentMap newConf = ((IPersistentMap) stormConf).assoc("topology.workers", newval);
		return new Object[] { newConf, topology };
	}

	// do the best to write data
	// unfortunately exceptions from here need a way to be handled as this
	// will likely be called from an asynchronous thread
	private void writePersistentData() {
		File pdfile = new File(_datadir, "data." + System.currentTimeMillis() + ".ser");

		synchronized (data) {
			try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(pdfile))) {
				oos.writeObject(data);
			} catch (IOException e) {
				// file write failed, it is an error, but recover old data
				try {
					if (pdfile.exists())
						FileUtils.forceDelete(pdfile);
				} catch (IOException e1) {
					throw new RuntimeException("Nonrecoverable, manageing persistent data files: "
							+ pdfile.getAbsolutePath() + ", original error=" + e.getLocalizedMessage() + ", additional="
							+ e1.getLocalizedMessage(), e);
				}
				throw new RuntimeException("Nonrecoverable, while writing data file: " + pdfile.getAbsolutePath(), e);
			}
		}

		for (String fn : _datadir.list()) {
			if (fn.startsWith("data.") && !fn.equals(pdfile.getName())) {
				File oldf = new File(_datadir, fn);
				try {
					FileUtils.forceDelete(oldf);
				} catch (IOException e) {
					throw new RuntimeException(
							"Nonrecoverable, unable to clean up older data files: " + oldf.getAbsolutePath(), e);
				}
			}
		}
	}

	// Do the best to read and/or recover the persistent data
	// exceptions from here should abort the deployment
	private void readPersistentData(boolean create) {

		TreeSet<String> reverseList = new TreeSet<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o2.compareTo(o1);
			}
		});
		reverseList.addAll(Arrays.asList(_datadir.list()));

		RuntimeException savede = null;
		for (String fn : reverseList) {
			if (fn.startsWith("data.")) {
				File pdfile = new File(_datadir, fn);
				try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(pdfile))) {
					data = (PersistentData) ois.readObject();
					return;
				} catch (Exception e) {
					savede = new RuntimeException(
							"Nonrecoverable error: while trying to read: " + pdfile.getAbsolutePath(), e);
				}
			}
		}
		if (savede != null)
			throw savede;

		if (!create)
			throw new RuntimeException("Nonrecoverable, unable to recover previous state.");

		data = new PersistentData();
	}

	private Map<Object, Double> calculateParallelism(Map<?, ?> metrics) throws Exception {
		StormTopology topology = _nimbus.getTopology(_stormId);
		Map<String, Bolt> bolts = topology.get_bolts();
		Map<String, SpoutSpec> spouts = topology.get_spouts();

		Map<Object, Double> parallelism = new HashMap<Object, Double>();

		List<Object> boltIdList = new ArrayList<Object>();
		Iterator<? extends Entry<?, ?>> itr = metrics.entrySet().iterator();
		while (itr.hasNext()) {
			Entry<?, ?> entry = itr.next();
			Object id = entry.getKey();
			if (bolts.containsKey(id)) {
				boltIdList.add(id);
			} else if (spouts.containsKey(id)) {
				Map<?, ?> spoutData = (Map<?, ?>) metrics.get(id);
				int upTime = (Integer) spoutData.get(":uptime");

				Iterator<? extends Entry<?, ?>> spoutDataItr = spoutData.entrySet().iterator();
				while (spoutDataItr.hasNext()) {
					Entry<?, ?> e = spoutDataItr.next();

					Object key = e.getKey();
					if (!":uptime".equals(key)) {

						long emitted = getLong((Map<?, ?>) e.getValue(), ":emitted", "600");
						long acked = getLong((Map<?, ?>) e.getValue(), ":acked", "600");
						// System.out.println(id + "-->" + (emitted / upTime) +
						// "-->" + (acked / upTime));

						// System.out.println(emitted - acked);

						int window = window(upTime);

						double currentTps = acked * 1d / window;
						System.out.println("CurrentTps = " + currentTps);
						System.out.println("TargetTps = " + targetTps);

						double scaleFactor = (emitted * 1d / acked);
						if (targetTps > currentTps) {
							scaleFactor *= (targetTps / currentTps);
						}
						System.out.println("ScaleFactor = " + scaleFactor);
						parallelism.put(new GlobalStreamId(id.toString(), key.toString()), scaleFactor);
					}
				}
			}
		}

		for (Object boltId : boltIdList) {
			Object id = boltId;
			double capacity = getDouble(metrics, id, ":capacity", "600");
			// System.out.println(capacity);
			// if (capacity > 0.8d) {
			Map<?, ?> boltData = (Map<?, ?>) metrics.get(id);
			Iterator<? extends Entry<?, ?>> boltDataItr = boltData.entrySet().iterator();
			long totalEmitted = 0;
			long totalExecuted = 0;
			int spoutSF = 1;
			while (boltDataItr.hasNext()) {
				Entry<?, ?> boltDataEntry = boltDataItr.next();

				if (boltDataEntry.getKey() instanceof GlobalStreamId) {
					GlobalStreamId gsd = (GlobalStreamId) boltDataEntry.getKey();
					String sComponent = gsd.get_componentId();
					Map<?, ?> sData = (Map<?, ?>) getProperty(metrics, sComponent, gsd.get_streamId());
					totalEmitted += getLong(sData, ":emitted", "600");
					totalExecuted += getLong((Map<?, ?>) boltDataEntry.getValue(), ":executed", "600");
					if (parallelism.containsKey(gsd)) {
						spoutSF *= parallelism.get(gsd);
					}
				}
			}

			if (totalExecuted > 0) {
				double scaleFactor = totalEmitted * 1d / totalExecuted;
				// System.out.println(totalEmitted + "->" + totalExecuted);
				// System.out.println(id + "->" + scaleFactor);

				parallelism.put(id, (scaleFactor * spoutSF));
			}
			// }
		}

		System.out.println(parallelism);
		// Spout pe

		return parallelism;
	}

	private Map<?, ?> getMetrics() throws Exception {

		GetInfoOptions options = new GetInfoOptions();
		options.set_num_err_choice(NumErrorsChoice.ONE);
		StormTopology topology = _nimbus.getTopology(_stormId);
		TopologyInfo info = _nimbus.getTopologyInfoWithOpts(_stormId, options);

		// Map<String, Bolt> bolts = topology.get_bolts();
		Map<String, SpoutSpec> spouts = topology.get_spouts();

		Map<?, ?> cache = newCache();

		List<ExecutorSummary> summaries = info.get_executors();

		Map<String, Double> capacityMap = new HashMap<String, Double>();

		for (ExecutorSummary summary : summaries) {
			ExecutorStats stats = summary.get_stats();
			if (stats == null) {
				continue;
			}

			int upTime = summary.get_uptime_secs();

			int window = window(upTime);

			String componentId = summary.get_component_id();
			boolean isSpout = spouts.containsKey(componentId);

			updateCache(cache, componentId, preprocess(stats.get_emitted()), null, ":emitted", null);
			updateCache(cache, componentId, preprocess(stats.get_transferred()), null, ":transferred", null);

			if (!isSpout) {

				// bolt
				BoltStats boltStats = stats.get_specific().get_bolt();
				Map<String, Map<GlobalStreamId, Double>> executeMsAvg = preprocess(boltStats.get_execute_ms_avg());
				Map<String, Map<GlobalStreamId, Long>> executed = preprocess(boltStats.get_executed());
				updateCache(cache, componentId, executed, executeMsAvg, ":executed", ":execute-latencies");

				Map<String, Map<GlobalStreamId, Double>> processMsAvg = boltStats.get_process_ms_avg();
				Map<String, Map<GlobalStreamId, Long>> acked = boltStats.get_acked();
				updateCache(cache, componentId, acked, processMsAvg, ":acked", ":process-latencies");

				if (window > 0) {
					// (div (* executed latency) (* 1000 window))

					Map<?, ?> capacityCache = newCache();
					updateCache(capacityCache, componentId, executed, executeMsAvg, ":executed", ":execute-latencies");

					capacityCache = (Map<?, ?>) capacityCache.get(componentId);
					Iterator<?> itr = capacityCache.values().iterator();
					long totalCount = 0;
					double totalLatency = 0;
					while (itr.hasNext()) {
						Map<?, ?> next = (Map<?, ?>) itr.next();
						Long count = getLong(next, ":executed", "600");
						Double latency = getDouble(next, ":execute-latencies", "600");
						totalLatency += (count * latency);
						totalCount += count;
					}

					if (totalCount > 0) {
						totalLatency /= totalCount;
					}

					if (!capacityMap.containsKey(componentId)) {
						capacityMap.put(componentId, 0d);
					}

					double capacity = capacityMap.get(componentId);
					double currCapacity = (totalCount * totalLatency) / (1000 * window);
					if (currCapacity > capacity) {
						capacity = currCapacity;
						setProperty(cache, capacity, componentId, ":capacity", "600");
						capacityMap.put(componentId, capacity);
					}
				}
			} else if (isSpout) {
				// spout

				SpoutStats spoutStats = stats.get_specific().get_spout();
				updateCache(cache, componentId, preprocess(spoutStats.get_acked()), spoutStats.get_complete_ms_avg(),
						":acked", ":complete-latencies");
				updateCache(cache, componentId, preprocess(spoutStats.get_failed()), null, ":failed", null);
				setProperty(cache, summary.get_uptime_secs(), componentId, ":uptime");
			}
		}

		// System.out.println(cache);

		return cache;
	}

	private Map<?, ?> newCache() {
		return LazyMap.decorate(new HashMap<String, Object>(), new Transformer() {
			@Override
			public Object transform(Object o) {
				if (o instanceof String) {
					String s = (String) o;
					if (":all-time".equalsIgnoreCase(s) || ":uptime".equalsIgnoreCase(s) || "600".equals(s)
							|| "10800".equals(s) || "86400".equals(s)) {
						return (Number) 0;
					}
				}
				return LazyMap.decorate(new HashMap<String, Object>(), this);
			}
		});
	}

	private <T> void updateCache(Map<?, ?> cache, String componentId, Map<String, Map<T, Long>> countMap,
			Map<String, Map<T, Double>> latenciesMap, String cacheCountPropKey, String cacheLatencyPropKey)
					throws Exception {
		Iterator<Entry<String, Map<T, Long>>> entries = countMap.entrySet().iterator();
		while (entries.hasNext()) {
			Entry<String, Map<T, Long>> entry = entries.next();
			String key = entry.getKey();
			Map<T, Long> valueMap = entry.getValue();

			Iterator<Entry<T, Long>> itr = valueMap.entrySet().iterator();
			while (itr.hasNext()) {

				Entry<T, Long> e = itr.next();
				T gsId = e.getKey();

				// <componentId>.<executed>.<slice>
				Object[] countProp = new Object[] { componentId, gsId, cacheCountPropKey, key };
				Object[] latencyProp = null;
				if (cacheLatencyPropKey != null) {
					latencyProp = new Object[] { componentId, gsId, cacheLatencyPropKey, key };
				}

				Long thisTotal = e.getValue();

				if (thisTotal != null) {
					long currentTotal = getLong(cache, countProp);
					long newTotal = thisTotal + currentTotal;
					setProperty(cache, newTotal, countProp);

					if (latencyProp != null) {
						double currentLatency = getDouble(cache, latencyProp);
						double thisLatency = getDouble(latenciesMap, key, gsId);
						setProperty(cache, ((currentLatency * currentTotal + thisTotal * thisLatency) / newTotal),
								latencyProp);
					}
				}
			}
		}
	}

	private Object getProperty(Map<?, ?> cache, Object... keys) {
		for (int i = 0; i < keys.length - 1; i++) {
			cache = (Map<?, ?>) cache.get(keys[i]);
		}
		return ((Map<?, ?>) cache).get(keys[keys.length - 1]);
	}

	@SuppressWarnings({ "unchecked" })
	private void setProperty(Map<?, ?> cache, Object value, Object... keys) {
		for (int i = 0; i < keys.length - 1; i++) {
			cache = (Map<?, ?>) cache.get(keys[i]);
		}
		((Map<Object, Object>) cache).put(keys[keys.length - 1], value);
	}

	private <T, V extends Number> Map<String, Map<T, V>> preprocess(Map<String, Map<T, V>> m) {
		Iterator<Entry<String, Map<T, V>>> itr = m.entrySet().iterator();
		Map<String, Map<T, V>> newMap = new HashMap<String, Map<T, V>>();
		while (itr.hasNext()) {
			Entry<String, Map<T, V>> e = itr.next();
			newMap.put(e.getKey(), filterSys(e.getValue()));
		}
		return newMap;
	}

	private <T, V extends Number> Map<T, V> filterSys(Map<T, V> m) {
		Map<T, V> newMap = new HashMap<T, V>(m);
		Iterator<T> itr = newMap.keySet().iterator();
		while (itr.hasNext()) {
			T key = itr.next();
			String keyStr = null;
			if (key instanceof String) {
				keyStr = (String) key;
			} else if (key instanceof GlobalStreamId) {
				keyStr = ((GlobalStreamId) key).get_streamId();
			}

			if (Utils.isSystemId(keyStr)) {
				itr.remove();
			}
		}
		return newMap;
	}

	private Number getNumber(Map<?, ?> metrics, Object... keys) {
		Object o = getProperty(metrics, keys);
		if (o instanceof Number) {
			return ((Number) o);
		}
		throw new RuntimeException();
	}

	private double getDouble(Map<?, ?> metrics, Object... keys) {
		return getNumber(metrics, keys).doubleValue();
	}

	private long getLong(Map<?, ?> metrics, Object... keys) {
		return getNumber(metrics, keys).longValue();
	}

	private int window(int upTime) {
		return (upTime < 600) ? upTime : 600;
	}

}
