package hadoop.mrwage.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DepartmentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	private Double wageHR = new Double(0);
	private Double wageAcc = new Double(0);
	private MultipleOutputs<Text, DoubleWritable> output;
	
	@Override
	protected void setup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		output = new MultipleOutputs<Text, DoubleWritable>(context);
	}
	
	@Override
	protected void reduce(Text departmentCode, Iterable<DoubleWritable> wages,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		for (DoubleWritable w : wages) {
			if (departmentCode.toString().equalsIgnoreCase("human resources")) {
				wageHR += w.get();
			} else {
				wageAcc += w.get();
			}
		}
		if (departmentCode.toString().equalsIgnoreCase("human resources")) {
			output.write("human resources", departmentCode, wageHR);
		} else {
			output.write("accounting", departmentCode, wageAcc);
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		output.close();
	}
	
}
