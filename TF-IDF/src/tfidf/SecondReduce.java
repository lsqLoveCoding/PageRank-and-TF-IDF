package tfidf;
 
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class SecondReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
 
	@Override
	protected void reduce(Text w, Iterable<IntWritable> itr,Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : itr) {
			sum +=i.get();
		}
		context.write(w, new IntWritable(sum));
	}
}