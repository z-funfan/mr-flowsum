package xyz.funfan.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import xyz.funfan.mr.flowsum.model.FlowBean;

public class FlowSumDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if (System.getProperty("os.name").startsWith("Windows")) {
			System.setProperty("hadoop.home.dir", "D:\\Study\\hadoop\\hadoop-2.8.3");
		}

		int exitCode = ToolRunner.run(new FlowSumDriver(), args);
        System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
			return -1;
		}
		
		Configuration conf = new Configuration();
	
		// 1. Init Job for Flow sum
		Job flowSumJob = new Job(conf, "FlowSum");
		flowSumJob.setJarByClass(FlowSumDriver.class);
		flowSumJob.setJobName(getClass().getSimpleName());
	
		// Set Map-Reduce class
		flowSumJob.setMapperClass(FlowSumMapper.class);
		flowSumJob.setReducerClass(FlowSumReducer.class);
		
//		// Set partition
//		job.setPartitionerClass(FlowSumPartitioner.class);
//		job.setNumReduceTasks(5);
		
		// Set Reduce output format
		flowSumJob.setOutputKeyClass(Text.class);
		flowSumJob.setOutputValueClass(FlowBean.class);
		
		// Default inputFormat
		// job.setInputFormatClass(TextInputFormat.class);
		
		// Combined Input format
		flowSumJob.setInputFormatClass(CombineTextInputFormat.class);
		
		// Customized input format
		//job.setInputFormatClass(CompressedCombineFileInputFormat.class);
		
		FileInputFormat.setMaxInputSplitSize(flowSumJob, 256 * 1024 * 1024);
		FileInputFormat.setMinInputSplitSize(flowSumJob, 128 * 1024 * 1024);
		// Specified the input and output dir
		FileInputFormat.addInputPath(flowSumJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(flowSumJob, new Path(args[1]));
		
		// int returnValue = flowSumJob.waitForCompletion(true) ? 0:1;
		
		// new ControlledJob for first job
        ControlledJob ctrljobSum = new ControlledJob(conf);   
        ctrljobSum.setJob(flowSumJob);   
        
		// 2. Init second job for flow sort
        Job flowSortJob = new Job(conf, "FlowSort");
 		flowSortJob.setJarByClass(FlowSumDriver.class);
 		flowSortJob.setJobName(getClass().getSimpleName());
	
		// Set Map-Reduce class
 		flowSortJob.setMapperClass(SortFlowSumMapper.class);
 		flowSortJob.setReducerClass(SortFlowSumReducer.class);
		
		// Set Reduce output format
 		flowSortJob.setOutputKeyClass(FlowBean.class);
 		flowSortJob.setOutputValueClass(Text.class);
		
		// Combined Input format
 		flowSortJob.setInputFormatClass(CombineTextInputFormat.class);
		
		FileInputFormat.setMaxInputSplitSize(flowSortJob, 256 * 1024 * 1024);
		FileInputFormat.setMinInputSplitSize(flowSortJob, 128 * 1024 * 1024);
		// The input should be the output of flowSumJob
		FileInputFormat.addInputPath(flowSortJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(flowSortJob, new Path(args[1] + "_sorted"));
		
		// int returnValue = flowSortJob.waitForCompletion(true) ? 0:1;
		
		ControlledJob ctrljobSort = new ControlledJob(conf);   
		ctrljobSort.setJob(flowSortJob);   
		
		// 3. Add dependencies
		ctrljobSort.addDependingJob(ctrljobSum);   
		
		// 4. Create main job controller
        JobControl jobCtrl = new JobControl("flowController");   
      
        // add dependencies and 
        jobCtrl.addJob(ctrljobSum);   
        jobCtrl.addJob(ctrljobSort);   
  
        // 5. Start Jobs in a new thread
        Thread runJobControl = new Thread(jobCtrl);
        runJobControl.start();
        
        while (true) {
        	if (jobCtrl.allFinished()) {
        		int returnValue = jobCtrl.getFailedJobList().size() == 0 ? 0 : 1;
        		if (returnValue == 0) {
        			System.out.println("Job was successful");
        		} else {
        			System.out.println("Job was failed");
        		}
        		return returnValue;
        	} else {
        		Thread.sleep(1000);
        	}
        }
	}
}
