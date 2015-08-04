package backtype.storm.autoscale.throughput;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.autoscale.IAutoScaler;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.generated.Nimbus.Iface;

/**
 * An auto-scale algorithm designed to manage the topology primarily by
 * capacity/throughput metrics.
 *
 */
public class ThroughputAutoScaler implements IAutoScaler {

	private static class PersistentData implements Serializable {

	}

	private static final Logger LOG = LoggerFactory.getLogger(ThroughputAutoScaler.class);

	private File _datadir;
	private PersistentData data;

	@Override
	public void startAutoScaler(Iface nimbus, File datadir, final String stormName, String stormId,
			@SuppressWarnings("rawtypes") Map totalStormConf, StormTopology topology) {

		_datadir = datadir;
		data = readPersistentData();

		new Thread(this.getClass().getSimpleName() + "-" + stormName) {
			@Override
			public void run() {
				try {
					while (true) {
						try {

							writePersistentData();
							// get from config
							Thread.sleep(10000);
						} catch (Exception e) {
							LOG.error("Something recoverable happend for autoscaler for topo=" + stormName + " : "
									+ e.getLocalizedMessage(), e);
						}
					}
				} catch (Throwable t) {
					LOG.error("The autoscaler for: " + stormName + " has failed, system must exit: "
							+ t.getLocalizedMessage());
					System.exit(1); // fail fast.
				}
			}

		}.start();
	}

	@Override
	public void configureTopology(String stormName, String stormId, @SuppressWarnings("rawtypes") Map totalStormConf,
			StormTopology topology) {
	
		LOG.info("Spouts:");
		for (Entry<String, SpoutSpec> ent : topology.get_spouts().entrySet()) {
			SpoutSpec spec = ent.getValue();
			LOG.info("  " + ent.getKey() + " " + spec.get_spout_object().getSetField());
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
			LOG.info("  " + ent.getKey() + " " + spec.get_bolt_object().getSetField());
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
	
	}

	// do the best to write data
	// unfortunately exceptions from here need a way to be handled as this
	// will likely be called from an asynchronous thread
	private void writePersistentData() {
		File pdfile = new File(_datadir, "data.ser");
		File pdbackfile = new File(_datadir, "data.bak");

		if (pdfile.exists()) {
			try {
				if (pdbackfile.exists())
					FileUtils.forceDelete(pdbackfile);
				FileUtils.moveFile(pdfile, pdbackfile);
			} catch (IOException e) {
				throw new RuntimeException("Nonrecoverable, manageing persistent data files: "
						+ pdfile.getAbsolutePath() + " and " + pdbackfile.getAbsolutePath(), e);
			}
		}

		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(pdfile))) {
			oos.writeObject(data);
			return;
		} catch (IOException e) {
			// file write failed, it is an error, but recover old data
			try {
				if (pdfile.exists())
					FileUtils.forceDelete(pdfile);
				if (pdbackfile.exists())
					FileUtils.moveFile(pdbackfile, pdfile);
			} catch (IOException e1) {
				throw new RuntimeException("Nonrecoverable, manageing persistent data files, recovery: "
						+ pdfile.getAbsolutePath() + " and " + pdbackfile.getAbsolutePath(), e1);
			}
			throw new RuntimeException("Nonrecoverable, while writing data file: " + pdfile.getAbsolutePath(), e);
		}
	}

	// Do the best to read and/or recover the persistent data
	// exceptions from here should abort the deployment
	private PersistentData readPersistentData() {
		File pdfile = new File(_datadir, "data.ser");
		if (pdfile.exists()) {
			try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(pdfile))) {
				return (PersistentData) ois.readObject();
			} catch (Exception e) {
				LOG.error("Recoverable error: while trying to read: " + pdfile.getAbsolutePath(), e);
				try {
					if (pdfile.exists())
						FileUtils.forceDelete(pdfile);
				} catch (IOException e1) {
					throw new RuntimeException(
							"Nonrecoverable, unable to delete corrupt data file: " + pdfile.getAbsolutePath(), e1);
				}
			}
		}

		// problems with main file
		File pdbackfile = new File(_datadir, "data.bak");
		if (pdbackfile.exists()) {
			try {
				FileUtils.moveFile(pdbackfile, pdfile);
			} catch (IOException e) {
				LOG.error("Recoverable, unable to recover backup file: " + pdbackfile.getAbsolutePath(), e);
				try {
					if (pdbackfile.exists())
						FileUtils.forceDelete(pdbackfile);
				} catch (IOException e1) {
					throw new RuntimeException(
							"Nonrecoverable, unable to delete corrupt backup data file: " + pdfile.getAbsolutePath(),
							e1);
				}
			}
			try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(pdfile))) {
				return (PersistentData) ois.readObject();
			} catch (Exception e) {
				LOG.error("Recoverable error: while trying to recover and read: " + pdfile.getAbsolutePath(), e);
				try {
					if (pdfile.exists())
						FileUtils.forceDelete(pdfile);
				} catch (IOException e1) {
					throw new RuntimeException(
							"Nonrecoverable, unable to delete corrupt recovered data file: " + pdfile.getAbsolutePath(),
							e1);
				}
			}
		}

		return new PersistentData();
	}

}
