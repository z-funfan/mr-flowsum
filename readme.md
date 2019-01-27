# mr-flowsum

这是我的第二个Map-Reduce程序。
主要功能如下：

- 统计数据文件中各个手机号的流量
- 通过流量使用情况倒序
- 按照手机号前三位分区， 130~139， 150~159， 180~189

这个程序主要用来练习，Map-Reduce的自定义序列化类, TextInputFormat，分区以及排序操作

## 自定义序列化类

仅仅使用MR的基本类型处理业务逻辑肯定是不够的，
和普通JAVA程序类似，MR程序可以需要创建与Entity或者DomainModel类似的自定义类。
Hadoop框架中，需要大量的永达哦网路传输，或者文件读写，
因此Hadoop框架定义了一套相对于Serializable更为轻量，更为高效的序列化方法，
在我们使用时只要实现Hadoop的Writable接口即可

```java

public class FlowBean implements Writable {

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
```

## TextInputFormat

当输入文件是众多小文件时，可以通过Combiner来优化Hadoop处理效率。
Hadoop的默认处理方式是，为每个文件都会单独创建一个Map task来切分数据，
当输入文件都特别小时候，就很浪费资源，处理效率也低，因为即使再小的文件，
Hadoop也会给文件分配128MB的块来处理（这个128MB是HDFS的默认配置）。

这种情况下，一般有两种优化方式，获取输入文件的时候，就把文件处理成大小适中的块；
第二种，就是将默认的TextInputFormat替换为CombineFileInputFormat的实现。

你可以自己实现抽象类CombineFileInputFormat中的方法，
或者hadoop-mapreduce-client-core中提供了一个实现类CombineTextInputFormat，
我们可以直接使用这个类来完成小文件的合并。

自定义InputFormat比较复杂，可以参考这篇文章[Process small, compressed files in Hadoop using CombineFileInputFormat](https://www.ibm.com/developerworks/library/bd-hadoopcombine/index.html)

创建完CombineTextInputFormat类之后，只要在MapReduce驱动类中指定对应的InputFormat就能生效

```java
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

import xyz.funfan.mr.flowsum.combiner.CompressedCombineFileInputFormat;
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
	
		// New Job
		Job job = new Job();
		job.setJarByClass(FlowSumDriver.class);
		job.setJobName(getClass().getSimpleName());
	
		// Set Map-Reduce class
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		// Set Reduce output format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		// Default inputFormat
		// job.setInputFormatClass(TextInputFormat.class);
		
		// Combined Input format
		job.setInputFormatClass(CombineTextInputFormat.class);
		
		// Customized input format
		//job.setInputFormatClass(CompressedCombineFileInputFormat.class);
		
		FileInputFormat.setMaxInputSplitSize(job, 256 * 1024 * 1024);
		FileInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024);
		
//		// Set partition
//		job.setPartitionerClass(FlowSumPartitioner.class);
//		job.setNumReduceTasks(5);
		
		// Specified the input and output dir
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was failed");			
		}
		
		return returnValue;
	}

}

```

使用默认的TextInputFormat：

```
	Line 27: 信息: Task:attempt_local_0001_m_000000_0 is done. And is in the process of commiting
	Line 47: 信息: Task:attempt_local_0001_m_000001_0 is done. And is in the process of commiting
```

可以看到，在优化配置之前，会为了两个文件分别创建各自的map task

```
	Line 27: 信息: Task:attempt_local_0001_m_000000_0 is done. And is in the process of commiting
	Line 45: 信息: Task:attempt_local_0001_r_000000_0 is done. And is in the process of commiting
```
修改后，根据设置的文件切分上下限（这里是128MB），将输入文件切分成合适的大小

## 分区

实际上分区已经在第一个例子mr-wordcount里做过了，就是继承Partitioner类定义自己的分区方法；
注意创建Driver类的时候，要制定Reduce Task的个数大于分区数，比如这里，至少要4个Task

## 排序

map-reduce程序默认情况下会 使用字典排序，一般没什么业务需求的情况下，这也够了。
当但在业务中时常会用到按价格排序，按使用量排序，按热度排序等等功能，这个时候，就需要自定义排序了。

最简单的不分区全局排序，就是在自定义类中实现WritableComparable接口，实现排序方法，将该类作为Mapper类的output key即可实现自定义排序。
我们修改一下上面提到过得FlowBean，让他实现排序接口

```java
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

```

但是这种方法有个局限性，即，全局排序，只能作用在Mapper Output Key上，例子中，如果将flowBean作为Mapper output Key,
在Reducer中获取对应的手机号，不能达到根据手机号累加流量的效果。
这种情况下，需要做两次MapReduce：第一次用手机号做Key，累加计算出流量总和，
再将第一个次Mapreduce的输出value（FlowBean）作为第二次map的key ，在第二次reduce再还原成原来的key value形式 

```java

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

```
## MR任务依赖

有时一个mapreduce程序时完成不了的，往往需要多个mapreduce程序，这个时候就要牵扯到各个任务之间的依赖关系，所谓依赖就是一个M/R Job 的处理结果是另外的M/R 的输入。向这个例子中，为了给流量统计并排序，就需要执行两个MR程序：

1. 第一个MR程序按手机号累加流量，输出每个手机号的流量总和
2. 第二个MR程序，将第一个MR程序的输出作为输入，并计算排序
3. 最终得到排序过后的结果

最笨的方法，我们当然可以等待Job1完成之后，自己新建一个新的Job2，读取Job1的结果来执行，像这样：

```java

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

public class FlowSumDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FlowSumDriver(), args);
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
		job.setJarByClass(FlowSumDriver.class);
		job.setJobName(getClass().getSimpleName());
	
		// Set Map-Reduce class
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		// Set Reduce output format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		// Default inputFormat
		// job.setInputFormatClass(TextInputFormat.class);
		
		// Combined Input format
		job.setInputFormatClass(CombineTextInputFormat.class);
		
		// Customized input format
		//job.setInputFormatClass(CompressedCombineFileInputFormat.class);
		
		FileInputFormat.setMaxInputSplitSize(job, 256 * 1024 * 1024);
		FileInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024);	
		
		// Specified the input and output dir
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			if (this.runSecond(args) == 0) {
				System.out.println("Job was successful");
			} else {
				System.out.println("Job was failed");			
			}
		} else if(!job.isSuccessful()) {
			System.out.println("Job was failed");			
		}
		
		return returnValue;
	}
	
	private int runSecond(String[] args) throws Exception {
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
		FileInputFormat.addInputPath(job, new Path(args[1] + "/part*"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_sort"));
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Sencondary Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Sencondary Job was failed");			
		}
		
		return returnValue;
	}
}


```

如果想写的更优雅一点，或者为了能够适应更复杂的以来情况，
Hadoop提供了任务控制容器ControlledJob，我们只要根据需要，在驱动类中加入ControlledJob 相关配置即可.
相比于原来的驱动类：

1. 和原来的驱动类一样，分别创建对应的MR Job
2. 将为Job1， Job2分别创建对应的ControlledJob容器， ctrljobSum, ctrljobSort
3.  通过 `ControlledJob.addDependingJob` 方法ctrljobSum, ctrljobSort配置依赖关系
4. 创建主题JobControl，通过控制器 来控制ControlledJob
5. 创建新线程，启动JobControl，通过`JobControl.getFailedJobList()`能够获取失败的任务

```java
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

```
