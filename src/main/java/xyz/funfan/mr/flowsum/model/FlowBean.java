package xyz.funfan.mr.flowsum.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {

	private long upFlow;
	
	private long downFlow;
	
	private long flowSum;
	
	public FlowBean() {
		super();
	}
	
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.flowSum = upFlow + downFlow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.flowSum = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(flowSum);
	}
	
	@Override
	public int compareTo(FlowBean o) {
		if (this.flowSum > o.getFlowSum()) {
			return -1;
		} else {
			return 1;
		}
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getFlowSum() {
		return flowSum;
	}

	public void setFlowSum(long flowSum) {
		this.flowSum = flowSum;
	}

	@Override
	public String toString() {
		return String.valueOf(this.upFlow).concat("\t")
				.concat(String.valueOf(this.downFlow)).concat("\t")
				.concat(String.valueOf(this.flowSum));
	}
}
