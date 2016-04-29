package neu.edu.yelp.recommendation;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {

	Map<String, Double> personMap = new HashMap<>();
	Map<String, HashMap<String, Double>> usersMap = new HashMap<>();
	HashMap<String, Double> map = null;
	String person;

	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			if (line != null && line.length() > 0) {// user user item stars
				String[] words = line.split(" ");
				if (words[0].equals(words[1])) {
					person = words[1];
					personMap.put(words[2], Double.parseDouble(words[3]));
				}
				if (personMap.containsKey(words[2]) && personMap != null && !words[0].equals(words[1])) {
					context.write(new Text(person + " " + words[0]), new Text(
							words[2] + " " + +personMap.get(words[2]) + " "
									+ words[3]));//person user, business personRating userRating
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
