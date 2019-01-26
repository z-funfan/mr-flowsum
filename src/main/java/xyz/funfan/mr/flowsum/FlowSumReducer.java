package xyz.funfan.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import xyz.funfan.mr.flowsum.model.FlowBean;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		long upFlowSum = 0;
		long downFlowSum = 0;
		
		for (FlowBean value: values) {
			upFlowSum = upFlowSum + value.getUpFlow();
			downFlowSum = downFlowSum + value.getDownFlow();
		}
		
		context.write(key, new FlowBean(upFlowSum, downFlowSum));
	}
}
