package hadoop.mrwage.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import hadoop.mrwage.mapper.DepartmentMapper;
import hadoop.mrwage.reducer.DepartmentReducer;

public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path input = new Path("file:////home/rodrir23/mo.txt/");
		Path output = new Path("file:////home/rodrir23/output/");
		
		Configuration configuration = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "DEPARTMENT WAGE JOB");
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(DepartmentMapper.class);
		job.setReducerClass(DepartmentReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, input);
		MultipleOutputs.addNamedOutput(job, "hr", TextOutputFormat.class, Text.class, DoubleWritable.class);
		MultipleOutputs.addNamedOutput(job, "accounting", TextOutputFormat.class, Text.class, DoubleWritable.class);
		FileOutputFormat.setOutputPath(job, output);
		
		output.getFileSystem(job.getConfiguration()).delete(output, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
