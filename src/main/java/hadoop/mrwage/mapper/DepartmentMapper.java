package hadoop.mrwage.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartmentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text text, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] line = text.toString().split(",");
		String departmentCode = line[2];
		Double wage = Double.parseDouble(line[3]);
		context.write(new Text(departmentCode), new DoubleWritable(wage));
	}
	
}
