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

	public static synchronized void setNimbus(Nimbus.Iface nimbus) {
		_nimbus = nimbus;
	}

	/**
	 * Take the current topology, and enhance it with all the auto-scale'ed
	 * configuration. If an auto-scaler parameter is defined for stormName, it
	 * will be started. Following that a call to IAutoScaler.configureTopology()
	 * will be made.
	 * 
	 * @param stormName
	 * @param stormId
	 * @param uploadedJarLocation
	 * @param totalStormConf
	 * @param topology
	 * @return
	 */
	public static synchronized StormTopology configureTopology(String stormName, String stormId,
			String uploadedJarLocation, @SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology) {
		IAutoScaler autoScaler = makeAndStartAutoScaler(stormName, stormId, totalStormConf, topology);

		return autoScaler.configureTopology(stormName, stormId, totalStormConf, topology);
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

		// now lets see if any are running we don't know about
		for (String stormId : stormIds) {
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
					makeAndStartAutoScaler(stormName, stormId, totalStormConf, null);
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
	}

	// topology is a flag, to indicate a new deployment, if not null - else
	// reload and restart, ugly, but lots of shared code
	private static IAutoScaler makeAndStartAutoScaler(String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology) {
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
					if (topology != null)
						autoScaler.startAutoScaler(_nimbus, datadir, stormName, stormId, totalStormConf, topology);
					else
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
				if (topology != null) {
					autoScaler.startAutoScaler(_nimbus, datadir, stormName, stormId, totalStormConf, topology);
					nameToId.put(stormName, stormId);
				} else {
					autoScaler.reLoadAndReStartAutoScaler(_nimbus, datadir, stormName, stormId, totalStormConf);
					nameToId.put(stormName, stormId);
				}
			}
		}
		return autoScaler;
	}

}
