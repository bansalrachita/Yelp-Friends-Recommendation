package neu.edu.yelp.recommendation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YelpTransitiveMatrixMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	String person = null;
	int degree = 3;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		person = context.getConfiguration().get("person",
				"qL7Astun3i7qwr2IL5iowA");
		degree = context.getConfiguration().getInt("degree", degree);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			String userId = line.split("\t")[0];
			int distance = Integer.parseInt(line.split("\t")[1].split(" ")[0]);
			if (distance > degree) {
				distance = 0; //don't consider users beyond degree in recommendation
			}
			context.write(new Text(person), new Text(userId + " " + distance));
			System.out.println(person + "\t" + userId + " " + distance);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
