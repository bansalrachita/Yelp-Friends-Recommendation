package neu.edu.yelp.recommendation;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemSimilarityReducer extends
		Reducer<TaggedKey, Text, NullWritable, Text> {
	ArrayList<String> bIds = new ArrayList<>();
	String person = null;

	public void reduce(TaggedKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		NullWritable nullKey = NullWritable.get();

		for (Text line : values) {
			Text word = new Text();
			System.out.println("start :" + line.toString() + "key :" + key.toString());
			if (line.toString().length() > 0) {
				if (line.toString().contains("Business")) {
					bIds.add(key.toString());
					person = line.toString().split(" ")[1];
				}

				if (bIds.contains(key.toString()) && line.toString().contains("Business")) {
					word.set(line.toString().split(" ")[1] + " " + person + " "
							+ key.toString() + " "
							+ line.toString().split(" ")[2]);
					context.write(nullKey, word);
					System.out.println("word if :" + word.toString());
				}
				else if(bIds.contains(key.toString()) && !line.toString().contains("Business")){
					word.set(line.toString().split(" ")[0] + " " + person + " "
							+ key.toString() + " "
							+ line.toString().split(" ")[1]);
					context.write(nullKey, word);
					System.out.println("word else : " + word.toString());
				}
				// UserId person businessId,stars
			}
		}
	}
}
