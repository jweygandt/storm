package backtype.storm.autoscale;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;

/**
 * A static "singleton" class for managing the auto-scalers on a per topology
 * basis. This current implementation will pull the Config.TOPOLOGY_AUTO_SCALER
 * class from Nimbus's classpath. Ideally future implementations should be able
 * to pull the implementation from the uploadedJarLocation's jar file, in a
 * careful way.
 *
 */
public class AutoScalerManager {

	// Note: to simplify implementation, even topologies without auto-scale will
	// have an IAutoScaler.NONE

	private static Nimbus.Iface _nimbus;

	// all methods are simply synchronized to keep complex state synced.
	// Also prevents interactions between nimbus.mk-assignments and
	// nimbus.iface.deploy
	private static Map<String, IAutoScaler> autoScalersByName = new HashMap<>();
	private static BiMap<String, String> nameToId = HashBiMap.create();

	/**
	 * @param nimbus A handle to the Nimbus.Iface for the running process.
	 */
	public static synchronized void setNimbus(Nimbus.Iface nimbus) {
		_nimbus = nimbus;
	}

	/**
	 * Initialize the auto scaler. This can (and should) be called before the
	 * toplogy is actually started. It can then be followed by
	 * modifyConfigAndTopology.
	 * 
	 * @param stormName
	 * @param stormId
	 * @param totalStormConf
	 */
	public static synchronized void initializeAutoScaler(String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf) {
		// must be a restart, get info and get config
		IAutoScaler autoscaler = autoScalersByName.get(stormName);
		if (autoscaler != null && autoscaler.isRunning()) {
			// This should never have happened
			// With no id, but autoscaler cached, one should
			// have todeploy first
			throw new IllegalStateException("sormId: " + stormId);
		}
		makeAndStartAutoScaler(stormName, stormId, totalStormConf);
	}

	/**
	 * Modify the configuration and toplogy.
	 * 
	 * @param stormId
	 * @param stormConf
	 * @param topology
	 * @return
	 */
	@SuppressWarnings({ "rawtypes" })
	public static synchronized Object[] modifyConfigAndTopology(String stormId, Map stormConf, StormTopology topology) {

		if (hasAutoScalerById(stormId)) {
			IAutoScaler autoScaler = autoScalersByName.get(nameToId.inverse().get(stormId));
			return autoScaler.modifyConfigAndTopology(stormId, stormConf, topology);
		} else {
			return new Object[] { stormConf, topology };
		}
	}

	/**
	 * Determine if the topology has an autoscaler. This will also start up the
	 * autoscaler if it is not running. The stormId should represent a running
	 * topology, as calls will be made to nimbus to determine topology
	 * information.
	 * 
	 * @param stormId
	 * @return
	 */
	public static synchronized boolean hasAutoScalerById(String stormId) {
		String stormName = nameToId.inverse().get(stormId);
		if (stormName == null) {
			checkinOnTopology(stormId);
			stormName = nameToId.inverse().get(stormId);
		}
		return autoScalersByName.get(stormName) != IAutoScaler.NONE;
	}

	/**
	 * Stop the auto scaler.
	 * 
	 * @param stormName
	 */
	public static synchronized void stopAutoScalerByName(String stormName) {
		IAutoScaler autoScaler = autoScalersByName.get(stormName);
		if (autoScaler != null) {
			nameToId.remove(stormName);
			autoScaler.stopAutoScaler();
		}
	}

	/**
	 * Stop the auto scaler.
	 * 
	 * @param stormId
	 */
	public static synchronized void stopAutoScalerById(String stormId) {
		String stormName = nameToId.inverse().get(stormId);
		IAutoScaler autoScaler = autoScalersByName.get(stormName);
		if (autoScaler != null) {
			nameToId.remove(stormName);
			autoScaler.stopAutoScaler();
		}
	}

	/**
	 * Nimbus's mk-assignments is regularly polled to determine if the WISB
	 * matches the WIRI. Whan a restart of Nimbus happens, it is the
	 * responsibility of this method to properly reload and restart the
	 * autoscaler for the topology.
	 * 
	 * Since the AutoScaler will "never die" during Nimbus's normal operation,
	 * it is not necessary for this method to continuously check to see if it is
	 * running.
	 * 
	 * @param stormIds
	 *            BE SURE THIS IS A COMPLETE LIST, autoscalers for topologies
	 *            not on this list will be cleaned up.
	 */
	public static synchronized void checkinOnTopologies(Collection<String> stormIds) {
		// first lets see if any have stopped
		Set<String> removedIdsByName = new HashSet<>();
		for (Entry<String, String> runningAutoScalerEnts : nameToId.entrySet()) {
			if (!stormIds.contains(runningAutoScalerEnts.getValue())) {
				String stormName = runningAutoScalerEnts.getKey();
				IAutoScaler autoScaler = autoScalersByName.get(stormName);
				removedIdsByName.add(stormName);
				if (autoScaler != null)
					autoScaler.stopAutoScaler();
			}
		}
		for (String stormName : removedIdsByName) {
			nameToId.remove(stormName);
		}

		// now lets see if any topologies are running we don't have an
		// autoscaler for
		for (String stormId : stormIds) {
			checkinOnTopology(stormId);
		}
	}

	private static void checkinOnTopology(String stormId) {
		String stormName = nameToId.inverse().get(stormId);
		if (stormName == null) {
			// must be a restart, get info and get config
			try {
				TopologyInfo tinfo = _nimbus.getTopologyInfo(stormId);
				stormName = tinfo.get_name();

				IAutoScaler autoscaler = autoScalersByName.get(stormName);
				if (autoscaler != null) {
					// This should never have happened
					// With no id, but autoscaler cached, one should
					// have to
					// deploy first
					throw new IllegalStateException("sormId: " + stormId);
				}

				JSONObject totalStormConf = (JSONObject) JSONValue.parse(_nimbus.getTopologyConf(stormId));
				makeAndStartAutoScaler(stormName, stormId, totalStormConf);
			} catch (ClassCastException | TException e) {
				// Really, this should never happen, but if it does log
				// and
				// continue, as it is not "standard" topo
				nameToId.put(stormName, stormId);
				autoScalersByName.put(stormName, IAutoScaler.NONE);
				Log.error("Should not have happend, ignoring: " + e.getLocalizedMessage(), e);
			}
		}
	}

	private static IAutoScaler makeAndStartAutoScaler(String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf) {
		String autoScalerClassName = (String) totalStormConf.get(Config.TOPOLOGY_AUTO_SCALER);
		IAutoScaler autoScaler;
		if (autoScalerClassName == null) {
			autoScalersByName.put(stormName, IAutoScaler.NONE);
			nameToId.put(stormName, stormId);
			autoScaler = IAutoScaler.NONE;
		} else {

			File datadir = FileUtils.getFile((String) totalStormConf.get(Config.STORM_LOCAL_DIR), "nimbus",
					"stormtopopersist", stormName, "auto-scaler-data");

			autoScaler = autoScalersByName.get(stormName);
			if (autoScaler == null) {

				try {
					FileUtils.forceMkdir(datadir);
				} catch (IOException e) {
					throw new RuntimeException("Problems making persistent directory for topology: " + stormName
							+ " dir=" + datadir.getAbsolutePath() + ":" + e.getLocalizedMessage(), e);
				}

				try {
					autoScaler = (IAutoScaler) Class.forName(autoScalerClassName).newInstance();
					autoScaler.reLoadAndReStartAutoScaler(_nimbus, datadir, stormName, stormId, totalStormConf);
					autoScalersByName.put(stormName, autoScaler);
					nameToId.put(stormName, stormId);
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					// OK for today, as we don't support user defined auto
					// scalers,
					throw new RuntimeException("Invalid " + Config.TOPOLOGY_AUTO_SCALER + ":" + e.getLocalizedMessage(),
							e);
				}
			} else {
				autoScaler.reLoadAndReStartAutoScaler(_nimbus, datadir, stormName, stormId, totalStormConf);
				nameToId.put(stormName, stormId);
			}
		}
		return autoScaler;
	}

}
