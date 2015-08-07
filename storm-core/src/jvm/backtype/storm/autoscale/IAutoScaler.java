package backtype.storm.autoscale;

import java.io.File;
import java.util.Map;

import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Iface;
import backtype.storm.generated.StormTopology;

/**
 * Interface for various auto-scaler algorithms. The class should have a default
 * constructor, and will be configured by Config.TOPOLOGY_AUTO_SCALER.
 *
 */
/**
 * @author jweygandt
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

		public boolean isRunning() {
			return false;
		};

		// @Override
		// public void startAutoScaler(Iface nimbus, File datadir, String
		// stormName, String stormId,
		// @SuppressWarnings("rawtypes") Map totalStormConf, StormTopology
		// topology) {
		// }
		//
		@Override
		public void reLoadAndReStartAutoScaler(Iface nimbus, File datadir, String stormName, String stormId,
				@SuppressWarnings("rawtypes") Map totalStormConf) {
		};

		@Override
		public Object[] modifyConfigAndTopology(String stormId, @SuppressWarnings("rawtypes") Map stormConf,
				StormTopology topology) {
			return new Object[] { stormConf, topology };
		};
	};

	// /**
	// * This will be called after the constructor and before any other methods.
	// * The auto-scaler should, ideally restore its state, and start running,
	// * probably in a background thread. If there is no state to restore, this
	// * method may create an initial state.
	// *
	// * Since there is no stop method for Nimbus, the containing process may be
	// * killed at any time, this class should persist its state whenever it
	// * changes it.
	// *
	// * @param nimbus
	// * @param datadir
	// * @param stormName
	// * @param stormId
	// * @param totalStormConf
	// * @param topology
	// */
	// void startAutoScaler(Nimbus.Iface nimbus, File datadir, String stormName,
	// String stormId,
	// @SuppressWarnings("rawtypes") Map totalStormConf, StormTopology
	// topology);

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

	boolean isRunning();

	/**
	 * Modify the config and/or topology. Please make a deepCopy of topology,
	 * and the Map is an IPersistentMap.
	 * 
	 * @param stormId
	 * @param stormConf
	 * @param topology
	 * @return
	 */
	Object[] modifyConfigAndTopology(String stormId, @SuppressWarnings("rawtypes") Map stormConf, StormTopology topology);

}
