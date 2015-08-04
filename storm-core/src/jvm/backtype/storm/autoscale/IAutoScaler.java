package backtype.storm.autoscale;

import java.io.File;
import java.util.Map;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;

/**
 * Interface for various auto-scaler algorithms. The class should have a default
 * constructor, and will be configured by Config.TOPOLOGY_AUTO_SCALER.
 *
 */
public interface IAutoScaler {

	/**
	 * This will be called after the constructor and before any other methods.
	 * The auto-scaler should, ideally restore its state, and start running,
	 * probably in a background thread.
	 * 
	 * Since there is no stop method, the containing process may be killed at
	 * any time, this class should persist its state whenever it changes it.
	 * 
	 * @param nimbus
	 * @param datadir
	 * @param stormName
	 * @param stormId
	 * @param totalStormConf
	 * @param topology
	 */
	void startAutoScaler(Nimbus.Iface nimbus, File datadir, String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology);

	/**
	 * Configure the topology.
	 * 
	 * @param stormName
	 * @param stormId
	 * @param totalStormConf
	 * @param topology
	 *            Mutable copy of the Topology.
	 */
	void configureTopology(String stormName, String stormId, @SuppressWarnings("rawtypes") Map totalStormConf,
			StormTopology topology);

}
