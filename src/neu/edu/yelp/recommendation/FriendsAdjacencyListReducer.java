package neu.edu.yelp.recommendation;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendsAdjacencyListReducer extends Reducer<Text, Text, Text, Text> {

	String person = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		person = context.getConfiguration().get("person",
				"qL7Astun3i7qwr2IL5iowA");
		System.out.println("Reduce1 setup called! " + person);
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text line : values) {
			Text word = new Text();
			String minDistance = line.toString().split(" ")[0];
			word.set(line.toString());
			if (key.toString().equals(person) && line.toString().split(" ").length>0) {
				String start = minDistance.replace(minDistance, "0");
				word.set(start + " " + line.toString().split(" ")[1]);
			}
//			System.out.println(key + "\t" + word);
			context.write(key, word);
		}

	}

}
