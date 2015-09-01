package backtype.storm.autoscale.throughput;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import backtype.storm.generated.ExecutorSummary;

public abstract class CompData implements Serializable {

	static class StreamData {

		private final long targetTps;

		public StreamData(long targetTps) {
			this.targetTps = targetTps;
		}

		public long getPerInstanceTargetTps() {
			return targetTps;
		}

	}

	private static final long serialVersionUID = 1L;
	private transient List<ExecutorSummary> summaries = new LinkedList<>();

	public Object readResolve() {
		this.summaries = new LinkedList<>();
		return this;
	}

	public Iterable<ExecutorSummary> getSummaries() {
		return summaries;
	}

	public int getSummarySize() {
		return summaries.size();
	}

	public void beginUpdate() {
		summaries.clear();
	}
	
	public void postSummary(ExecutorSummary summary) {
		summaries.add(summary);
	}


	public abstract void updateInternals(PersistentData data);

	public abstract StreamData getStreamData(String streamId);
}
