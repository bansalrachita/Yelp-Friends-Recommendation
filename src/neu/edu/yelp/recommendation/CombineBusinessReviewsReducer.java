package neu.edu.yelp.recommendation;

import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombineBusinessReviewsReducer extends
		Reducer<TaggedKey, Text, NullWritable, Text> {

	ArrayList<String> bIds = new ArrayList<>();

	public void reduce(TaggedKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		NullWritable nullKey = NullWritable.get();

		for (Text line : values) {
			Text word = new Text();
//			System.out.println(key.toString() + " " + line);

			if (line.toString().length() > 0) {
				if (line.toString().contains("Las Vegas")) {
					bIds.add(key.toString());
				}

				if (bIds.contains(key.toString())) {
					word.set("{ \"business_id\": " + key.toString() + ", "
							+ line.toString() + " }");

					context.write(nullKey, word);
				}
			}
		}
	}

}
