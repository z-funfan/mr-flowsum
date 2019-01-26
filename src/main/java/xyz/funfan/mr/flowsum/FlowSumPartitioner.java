package xyz.funfan.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import xyz.funfan.mr.flowsum.model.FlowBean;

public class FlowSumPartitioner extends Partitioner<Text, FlowBean> {

	/**
	 * 130~139， 150~159， 180~189
	 */
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		int capital = Integer.parseInt(key.toString().substring(0, 3));
		if (capital >= 130 && capital <= 139) {
			return 0;
		} else if (capital >= 150 && capital <= 159) {
			return 1;
		} else if (capital >= 180 && capital <= 189) {
			return 2;
		} else {
			return 3;
		}
	}

}
