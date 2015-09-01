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

import java.util.Map.Entry;

import backtype.storm.generated.ExecutorSummary;

public class SpoutData extends CompData {
	private static final long serialVersionUID = 1L;

	private transient double computedParallelism;

	private String id;
	private int actualParallelism = 1;
	private Long targetTps;
	private Long throughput;

	public SpoutData() {
	}

	public Object readResolve() {
		return super.readResolve();
	}

	public void commitUpdate() {
	}

	public void setTargetTps(Long targetTps) {
		this.targetTps = targetTps;
	}

	public void setThroughput(Long throughput) {
		this.throughput = throughput;
	}

	public void setActualParallelism(int actualParallelism) {
		this.actualParallelism = actualParallelism;
	}

	public int getActualParallelism() {
		return actualParallelism;
	}

	public double getComputedParallelism() {
		if (getSummarySize() > 0)
			return computedParallelism;
		throw new RuntimeException("No summaries: " + id);
	}

	@Override
	public void updateInternals(PersistentData data) {
		if (getSummarySize() == 0)
			throw new RuntimeException("No summaries:" + id);

		long totalEmitted = 0;
		long windowSecs = data.getWindowtime();
		for (ExecutorSummary summary : getSummaries()) {
			for (Entry<String, Long> ent : summary.get_stats().get_emitted()
					.get(PersistentData.metricTimeWindowSecs + "").entrySet()) {
				String path = ent.getKey();
				if (path.startsWith("__"))
					continue;
				totalEmitted += ent.getValue();
			}
		}
		computedParallelism = (double) targetTps / (double) throughput;
		System.out.println("Spout: " + id + ", total_rate=" + ((double) totalEmitted / windowSecs) + ", compPism="
				+ computedParallelism + ", window=" + windowSecs);
	}

	@Override
	public StreamData getStreamData(String streamId) {
		// For today assume a basic entity like a Kafka spout, all data goes to
		// all streams
		return new StreamData(targetTps);
	}

}