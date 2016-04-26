package neu.edu.yelp.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YelpTransitiveMatrixReducer extends
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text line : values) {
			String userId = line.toString().split(" ")[0];
			String distance = line.toString().split(" ")[1];
			if(Integer.parseInt(distance) > 1){
				context.write(new Text(distance), new Text(key + " " + userId));
				System.out.println(distance + "\t" + key + " " + userId);
			}
			
		}
	}
}
