package tfidf;
 
import java.io.IOException;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ThirdReduce extends Reducer<Text, Text, Text, Text> {
 
	@Override
	protected void reduce(Text key, Iterable<Text> itr,Context context) throws IOException,
			InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		for (Text text : itr) {
			sb.append(text.toString()+"\t");
		}
		context.write(key, new Text(sb.toString()));
	}
}