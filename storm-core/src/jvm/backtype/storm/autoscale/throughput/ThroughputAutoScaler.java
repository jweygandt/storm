/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.autoscale.throughput;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.autoscale.IAutoScaler;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.BounceOptions;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GetInfoOptions;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.NumErrorsChoice;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.generated.TopologyInfo;
import clojure.lang.IPersistentMap;

/**
 * An auto-scale algorithm designed to manage the topology primarily by
 * capacity/throughput metrics.
 *
 */
public class ThroughputAutoScaler implements IAutoScaler {

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

						boolean dobounce;
						try {
							// get from config

							Thread.sleep(10000);
							updatePersistentData();
							dobounce = modifyParallelism();
						} catch (Exception e) {
							System.out.println("Ignored: " + e.getLocalizedMessage());
							e.printStackTrace();
							continue;
						}

						PersistentData.writePersistentData(_datadir, data);

						if (dobounce) {
							System.out.println("Bouncing topology.");
							BounceOptions options = new BounceOptions();
							options.set_step1_wait_secs(1);
							options.set_step2_wait_secs(10);
							_nimbus.bounce(_stormName, options);
						}

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
	private int supervisorNcores = 0;

	private volatile boolean done;
	private MonitorThread autoscaler = null;

	@Override
	public synchronized void reLoadAndReStartAutoScaler(Iface nimbus, File datadir, String stormName, String stormId) {
		_datadir = datadir;
		_nimbus = nimbus;
		_stormName = stormName;
		_stormId = stormId;
		done = false;
		data = PersistentData.readPersistentData(_datadir, true);

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

	@SuppressWarnings("unchecked")
	@Override
	public Object[] modifyConfigAndTopology(String stormId, @SuppressWarnings("rawtypes") Map stormConf,
			StormTopology topology) throws InvalidTopologyException {

		Object supervisorNcores = stormConf.get(TPASConfigExtensions.TOPOLOGY_TPAS_SUPERVISOR_NCORES);
		if (supervisorNcores == null)
			throw new InvalidTopologyException(
					"Need to specify: " + TPASConfigExtensions.TOPOLOGY_TPAS_SUPERVISOR_NCORES);
		this.supervisorNcores = ((Number) supervisorNcores).intValue();

		// Make a mutable copy
		topology = topology.deepCopy();

		synchronized (data) {
			LOG.info("Spouts:");
			for (Entry<String, SpoutSpec> ent : topology.get_spouts().entrySet()) {

				SpoutSpec spec = ent.getValue();
				String name = ent.getKey();

				Map<String, Map<String, Object>> spoutsTPASConfig = (Map<String, Map<String, Object>>) stormConf
						.get(TPASConfigExtensions.TOPOLOGY_TPAS_SPOUT_CONFIG);
				if (spoutsTPASConfig == null)
					throw new InvalidTopologyException("Must have " + TPASConfigExtensions.TOPOLOGY_TPAS_SPOUT_CONFIG);
				Map<String, Object> spoutTPASConfig = spoutsTPASConfig.get(name);
				if (spoutTPASConfig == null)
					throw new InvalidTopologyException("Spout " + name + " must have an entry in "
							+ TPASConfigExtensions.TOPOLOGY_TPAS_SPOUT_CONFIG);
				Number spoutTargetTPS = (Number) spoutTPASConfig
						.get(TPASConfigExtensions.TOPOLOGY_TPAS_SPOUT_CONFIG_TARGET_TPS);
				if (spoutTargetTPS == null)
					throw new InvalidTopologyException("Spout " + name + " must have a "
							+ TPASConfigExtensions.TOPOLOGY_TPAS_SPOUT_CONFIG_TARGET_TPS);
				Number spoutThroughput = (Number) spoutTPASConfig
						.get(TPASConfigExtensions.TOPOLOGY_TPAS_SPOUT_CONFIG_THROUGHPUT);
				if (spoutThroughput == null)
					throw new InvalidTopologyException("Spout " + name + " must have a "
							+ TPASConfigExtensions.TOPOLOGY_TPAS_SPOUT_CONFIG_THROUGHPUT);

				if (name.equals("spout1")) {
					// spec.get_common().set_parallelism_hint(newval);
					// spec.get_common().set_json_conf("{\"topology.tasks\" : "
					// +
					// newval + "}");
				}

				LOG.info("  " + name + " " + spec.get_spout_object().getSetField());
				ComponentCommon cc = spec.get_common();

				SpoutData spoutData = data.getSpout(name);
				spoutData.setTargetTps(spoutTargetTPS.longValue());
				spoutData.setThroughput(spoutThroughput.longValue());

				int parallelismHint = spoutData == null ? 1 : spoutData.getActualParallelism();

				// System.out.println("Using parallelism hint of " +
				// parallelismHint + " for " + ent.getKey());
				cc.set_parallelism_hint(parallelismHint);
				JSONObject jobj = (JSONObject) JSONValue.parse(cc.get_json_conf());
				jobj.put(Config.TOPOLOGY_TASKS, parallelismHint);
				cc.set_json_conf(jobj.toJSONString());

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
					// spec.get_common().set_json_conf("{\"topology.tasks\" : "
					// +
					// newval + "}");
				}

				LOG.info("  " + name + " " + spec.get_bolt_object().getSetField());
				ComponentCommon cc = spec.get_common();

				BoltData boltData = data.getBolts().get(name);
				int parallelismHint = boltData == null ? 1 : boltData.getActualParallelism();

				// System.out.println("Using parallelism hint of " +
				// parallelismHint + " for " + ent.getKey());
				cc.set_parallelism_hint(parallelismHint);
				JSONObject jobj = (JSONObject) JSONValue.parse(cc.get_json_conf());
				jobj.put(Config.TOPOLOGY_TASKS, parallelismHint);
				cc.set_json_conf(jobj.toJSONString());

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

			LOG.info("NWorkers: " + data.getNworkders());
			IPersistentMap newConf = ((IPersistentMap) stormConf).assoc("topology.workers", data.getNworkders());
			return new Object[] { newConf, topology };
		}
	}

	private void updatePersistentData() throws Exception {

		GetInfoOptions options = new GetInfoOptions();
		options.set_num_err_choice(NumErrorsChoice.ONE);
		TopologyInfo info = _nimbus.getTopologyInfoWithOpts(_stormId, options);
		List<ExecutorSummary> summaries = info.get_executors();

		data.beginUpdate();
		long uptime = Long.MAX_VALUE;
		for (ExecutorSummary summary : summaries) {
			ExecutorStats stats = summary.get_stats();
			if (stats == null) {
				continue;
			}
			data.postExecutorSummary(summary);
			uptime = Math.min(uptime, summary.get_uptime_secs());
		}
		data.commitUpdate(uptime);

		TopoGraph graph = new TopoGraph(data);
		for (CompData cd : graph.bredthFirstIterable()) {
			cd.updateInternals(data);
		}

	}

	private boolean modifyParallelism() {
		synchronized (data) {
			if (data.getUptime() < 20)
				return false;

			double totalPism = 0.0;
			for (BoltData bd : data.getBolts().values()) {
				totalPism += bd.getComputedParallelism();
			}
			for (SpoutData sd : data.getSpouts().values()) {
				totalPism += sd.getComputedParallelism();
			}
			double totalCores = Math.ceil(totalPism);
			int nhosts = (int) Math.ceil(totalCores / supervisorNcores);

			boolean modified = false;

			if (nhosts > data.getNworkders()) {
				data.setNworkders(nhosts);
				modified = true;
			}

			for (Entry<String, BoltData> ent : data.getBolts().entrySet()) {
				String id = ent.getKey();
				if (id.startsWith("__"))
					continue;
				BoltData bd = ent.getValue();
				int newPism = (int) (Math.ceil(bd.getComputedParallelism() / nhosts) * nhosts);
				if (newPism > bd.getActualParallelism()) {
					bd.setActualParallelism(newPism);
					modified = true;
				}
			}
			for (Entry<String, SpoutData> ent : data.getSpouts().entrySet()) {
				String id = ent.getKey();
				if (id.startsWith("__"))
					continue;
				SpoutData sd = ent.getValue();
				int newPism = (int) (Math.ceil(sd.getComputedParallelism() / nhosts) * nhosts);
				if (newPism > sd.getActualParallelism()) {
					sd.setActualParallelism(newPism);
					modified = true;
				}
			}
			return modified;
		}
	}

}
