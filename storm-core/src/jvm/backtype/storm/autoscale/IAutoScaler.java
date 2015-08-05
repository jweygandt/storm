package backtype.storm.autoscale;

import java.io.File;
import java.util.Map;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.Nimbus.Iface;

/**
 * Interface for various auto-scaler algorithms. The class should have a default
 * constructor, and will be configured by Config.TOPOLOGY_AUTO_SCALER.
 *
 */
public interface IAutoScaler {

	/**
	 * Used for topologies with manual scaling
	 */
	static final IAutoScaler NONE = new IAutoScaler() {

		@Override
		public void stopAutoScaler() {
		}

		@Override
		public void startAutoScaler(Iface nimbus, File datadir, String stormName, String stormId,
				@SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology) {
		}

		public void reLoadAndReStartAutoScaler(Iface nimbus, File datadir, String stormName, String stormId,
				@SuppressWarnings("rawtypes") Map totalStormConf) {
		};

		@Override
		public StormTopology configureTopology(String stormName, String stormId,
				@SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology) {
			return topology;
		}
	};

	/**
	 * This will be called after the constructor and before any other methods.
	 * The auto-scaler should, ideally restore its state, and start running,
	 * probably in a background thread. If there is no state to restore, this
	 * method may create an initial state.
	 * 
	 * Since there is no stop method for Nimbus, the containing process may be
	 * killed at any time, this class should persist its state whenever it
	 * changes it.
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
	 * This is typically the result of Nimbus restarting and picking up
	 * existing, running, topologies. There should be a state to restore from.
	 * 
	 * @param nimbus
	 * @param datadir
	 * @param stormName
	 * @param stormId
	 * @param totalStormConf
	 */
	void reLoadAndReStartAutoScaler(Nimbus.Iface nimbus, File datadir, String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf);

	/**
	 * This should shut down any activity for the stormId from the prior
	 * startAutoScaler() call.
	 */
	void stopAutoScaler();

	/**
	 * Configure the topology.
	 * 
	 * @param stormName
	 * @param stormId
	 * @param totalStormConf
	 * @param topology
	 *            Please make a deepCopy() before making changes
	 */
	StormTopology configureTopology(String stormName, String stormId, @SuppressWarnings("rawtypes") Map totalStormConf,
			StormTopology topology);

}
