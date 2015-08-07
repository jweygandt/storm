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
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.autoscale.IAutoScaler;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.BounceOptions;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
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

	private static class PersistentData implements Serializable {

		private static final long serialVersionUID = 1L;

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
							Thread.sleep(5000);
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

	// @Override
	// public synchronized void startAutoScaler(Iface nimbus, File datadir,
	// String stormName, String stormId,
	// @SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology)
	// {
	//
	// _datadir = datadir;
	// _nimbus = nimbus;
	// _stormName = stormName;
	// _stormId = stormId;
	// done = false;
	// readPersistentData(true);
	//
	// autoscaler = new MonitorThread(this.getClass().getSimpleName() + "-" +
	// stormName);
	// autoscaler.start();
	// }
	//
	@Override
	public synchronized void reLoadAndReStartAutoScaler(Iface nimbus, File datadir, String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf) {
		_datadir = datadir;
		_nimbus = nimbus;
		_stormName = stormName;
		_stormId = stormId;
		done = false;
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

		LOG.info("Spouts:");
		for (Entry<String, SpoutSpec> ent : topology.get_spouts().entrySet()) {

			SpoutSpec spec = ent.getValue();
			String name = ent.getKey();

			if (name.equals("spout1")) {
				spec.get_common().set_parallelism_hint(newval);
				spec.get_common().set_json_conf("{\"topology.tasks\" : " + newval + "}");
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
				spec.get_common().set_parallelism_hint(newval);
				spec.get_common().set_json_conf("{\"topology.tasks\" : " + newval + "}");
			}

			LOG.info("  " + name + " " + spec.get_bolt_object().getSetField());
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

}
