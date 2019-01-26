package xyz.funfan.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import xyz.funfan.mr.flowsum.model.FlowBean;

public class SortFlowSumReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Text value: values) {
			context.write(value, key);
		}
	}
}
