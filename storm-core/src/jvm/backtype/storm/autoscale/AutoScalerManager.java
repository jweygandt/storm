package backtype.storm.autoscale;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;

// Design thoughts: I would have liked to place this in nimbus.clj's nimbus config map, 
// like other helper instances are done. However this also needed a handle to the 
// Nimbus.Iface server. This presented a problem, and so the static-singleton.

/**
 * A static "singleton" class for managing the auto-scalers on a per topology
 * basis. This current implementation will pull the Config.TOPOLOGY_AUTO_SCALER
 * class from Nimbus's classpath. Ideally future implementations should be able
 * to pull the implementation from the uploadedJarLocation's jar file, in a
 * careful way.
 *
 */
public class AutoScalerManager {

	private static Nimbus.Iface _nimbus;
	private static Map<String, IAutoScaler> autoScalers = new HashMap<>();

	public static void setNimbus(Nimbus.Iface nimbus) {
		_nimbus = nimbus;
	}

	/**
	 * Take the current topology, and enhance it with all the auto-scale'ed
	 * configuration. If an auto-scaler exists for stormName a deepCopy of the
	 * topology will be made, modified and returned, else the original topology
	 * will be returned.
	 * 
	 * @param stormName
	 * @param stormId
	 * @param uploadedJarLocation
	 * @param totalStormConf
	 * @param topology
	 * @return
	 */
	public static StormTopology configureTopology(String stormName, String stormId, String uploadedJarLocation,
			@SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology) {
		String autoScalerClassName = (String) totalStormConf.get(Config.TOPOLOGY_AUTO_SCALER);
		if (autoScalerClassName == null)
			return topology;

		// Make a mutable copy
		topology = topology.deepCopy();

		IAutoScaler autoScaler;
		synchronized (autoScalers) {
			autoScaler = autoScalers.get(stormName);
			if (autoScaler == null) {

				File datadir = FileUtils.getFile((String) totalStormConf.get(Config.STORM_LOCAL_DIR), "nimbus",
						"stormtopopersist", stormName, "auto-scaler-data");

				try {
					FileUtils.forceMkdir(datadir);
				} catch (IOException e) {
					throw new RuntimeException("Problems making persistent directory for topology: " + stormName
							+ " dir=" + datadir.getAbsolutePath() + ":" + e.getLocalizedMessage(), e);
				}

				try {
					autoScaler = (IAutoScaler) Class.forName(autoScalerClassName).newInstance();
					autoScaler.startAutoScaler(_nimbus, datadir, stormName, stormId, totalStormConf, topology);
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					throw new RuntimeException("Invalid " + Config.TOPOLOGY_AUTO_SCALER + ":" + e.getLocalizedMessage(),
							e);
				}
			} else {
				// OK for today, as we don't support user defined auto scalers,
				// which may require reloading
			}
		}

		autoScaler.configureTopology(stormName, stormId, totalStormConf, topology);

		return topology;
	}

}
