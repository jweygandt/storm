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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;

import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;

public class PersistentData implements Serializable {
	private static final long serialVersionUID = 1L;

	static int metricTimeWindowSecs = 600;
	static String metricTimeWindowSecsStr = metricTimeWindowSecs + "";

	private transient long uptime = 0;

	private Map<String, BoltData> bolts = new HashMap<>();
	private Map<String, SysBoltData> sysbolts = new HashMap<>();
	private Map<String, SpoutData> spouts = new HashMap<>();
	private int nworkders = 1;

	public void beginUpdate() {
		for (BoltData bd : bolts.values()) {
			bd.beginUpdate();
		}
		for (SpoutData sd : spouts.values()) {
			sd.beginUpdate();
		}
	}

	public void commitUpdate(long uptime) {
		this.uptime = uptime;
		for (BoltData bd : bolts.values()) {
			bd.commitUpdate();
		}
		for (SpoutData sd : spouts.values()) {
			sd.commitUpdate();
		}
	}

	public long getUptime() {
		return uptime;
	}

	public long getWindowtime() {
		return Math.min(metricTimeWindowSecs, uptime);
	}

	public void postExecutorSummary(ExecutorSummary summary) {
		String componentId = summary.get_component_id();
		ExecutorStats stats = summary.get_stats();
		ExecutorSpecificStats specific = stats.get_specific();
		if (specific.is_set_bolt()) {
			if (componentId.startsWith("__")) {
				SysBoltData data = sysbolts.get(componentId);
				if (data == null) {
					data = new SysBoltData(componentId);
					sysbolts.put(componentId, data);
				}
				data.postSummary(summary);
			} else {
				BoltData data = bolts.get(componentId);
				if (data == null) {
					data = new BoltData(componentId);
					bolts.put(componentId, data);
				}
				data.postSummary(summary);
			}
		} else if (specific.is_set_spout()) {
			SpoutData data = spouts.get(componentId);
			if (data == null) {
				data = new SpoutData();
				spouts.put(componentId, data);
			}
			data.postSummary(summary);
		}
	}

	public CompData getComponent(String name) {
		CompData rv = bolts.get(name);
		if (rv != null)
			return rv;
		rv = spouts.get(name);
		if (rv != null)
			return rv;
		rv = sysbolts.get(name);
		if (rv != null)
			return rv;
		return null;
	}

	public Map<String, BoltData> getBolts() {
		return bolts;
	}

	public Map<String, SpoutData> getSpouts() {
		return spouts;
	}

	public SpoutData getSpout(String spoutName) {
		SpoutData sd = spouts.get(spoutName);
		if (sd == null) {
			sd = new SpoutData();
			spouts.put(spoutName, sd);
		}
		return sd;
	}

	public void setNworkders(int nworkders) {
		this.nworkders = nworkders;
	}

	public int getNworkders() {
		return nworkders;
	}

	// do the best to write data
	// unfortunately exceptions from here need a way to be handled as this
	// will likely be called from an asynchronous thread
	public static void writePersistentData(File _datadir, PersistentData data) {
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
	public static PersistentData readPersistentData(File _datadir, boolean create) {

		PersistentData data;
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
					return data;
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
		return data;
	}

}