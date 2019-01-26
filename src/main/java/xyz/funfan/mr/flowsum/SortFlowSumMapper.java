package xyz.funfan.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import xyz.funfan.mr.flowsum.model.FlowBean;

public class SortFlowSumMapper extends Mapper<LongWritable, Text,FlowBean, Text> {

	private Text phone = new Text();
 
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
        String[] fields = line.split("\t");
        
		String phoneNumber  = fields[0];
		this.phone.set(phoneNumber);
		long flowUp = Long.parseLong(fields[1]);
		long flowDown = Long.parseLong(fields[2]);
		
		context.write(new FlowBean(flowUp, flowDown), this.phone);
	}

}
