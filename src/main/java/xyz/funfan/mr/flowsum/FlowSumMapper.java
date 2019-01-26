package xyz.funfan.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import xyz.funfan.mr.flowsum.model.FlowBean;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	private Text phone = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
        String[] fields = line.split("\t");
        
        if (fields.length > 4) {
        	String phoneNumber  = fields[1];
        	this.phone.set(phoneNumber);
        	long flowUp = Long.parseLong(fields[fields.length - 3]);
        	long flowDown = Long.parseLong(fields[fields.length - 2]);
        	
        	context.write(this.phone, new FlowBean(flowUp, flowDown));
        }
	}

}
