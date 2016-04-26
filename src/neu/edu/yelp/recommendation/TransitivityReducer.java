package neu.edu.yelp.recommendation;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TransitivityReducer extends Reducer<Text, Text, Text, Text> {

	int infinity = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		infinity = context.getConfiguration().getInt("infinity", 999);
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String nodes = "UNMODED";
		Text word = new Text();
		int lowest = infinity; //
		for (Text line : values) { // looks like NODES/VALUES 1 0 2:3:, we need
									// to use the first as a key
			System.out.println("key -> " + key + "value -> " + line);
			String[] sp = line.toString().split(" "); // splits on space
			// look at first value
			if (sp[0].equalsIgnoreCase("NODES")) {
				nodes = null;
				nodes = sp[1];
			} else if (sp[0].equalsIgnoreCase("VALUE")) {
				int distance = Integer.parseInt(sp[1]);
				lowest = Math.min(distance, lowest);
			}
		}
		word.set(lowest + " " + nodes);
		System.out.println(key + "\t" + word);
		// don't write node with no connections
		if (!nodes.equals("UNMODED")) {
			context.write(key, word);
		}

		word.clear();
	}
}
