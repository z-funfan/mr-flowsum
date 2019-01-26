package xyz.funfan.mr.flowsum;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import xyz.funfan.mr.flowsum.model.FlowBean;

public class SortFlowSumDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if (System.getProperty("os.name").startsWith("Windows")) {
			System.setProperty("hadoop.home.dir", "D:\\Study\\hadoop\\hadoop-2.8.3");
		}

		int exitCode = ToolRunner.run(new SortFlowSumDriver(), args);
        System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
			return -1;
		}
	
		// New Job
		Job job = new Job();
		job.setJarByClass(SortFlowSumDriver.class);
		job.setJobName(getClass().getSimpleName());
	
		// Set Map-Reduce class
		job.setMapperClass(SortFlowSumMapper.class);
		job.setReducerClass(SortFlowSumReducer.class);
		
		// Set Reduce output format
		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(Text.class);
		
		
		// Combined Input format
		job.setInputFormatClass(CombineTextInputFormat.class);
		FileInputFormat.setMaxInputSplitSize(job, 256 * 1024 * 1024);
		FileInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024);
		
		// Combined Input format
		job.setInputFormatClass(CombineTextInputFormat.class);
		
		// Specified the input and output dir
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_sort"));
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was failed");			
		}
		
		return returnValue;
	}

}
