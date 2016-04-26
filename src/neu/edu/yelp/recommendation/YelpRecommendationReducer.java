package neu.edu.yelp.recommendation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YelpRecommendationReducer extends Reducer<Text, Text, Text, Text> {

	String person = null;

	Map<String, Integer> transitivityMap = new HashMap<>();
	Map<String, Double> similarityMap = new HashMap<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text line : values) {
			if (line != null && line.toString().length() > 0) {
//				System.out.println(line);
				if (line.toString().split(" ")[0].equals("d")) {
					Integer distance = Integer.parseInt(line.toString().split(
							" ")[1]);
					transitivityMap.put(key.toString(), distance);
					System.out.println(key + " " + distance);
				} else if (line.toString().split(" ")[0].equals("s")) {
					// similarity
					Double similarity = Double.parseDouble(line.toString()
							.split(" ")[1]);
//					System.out.println(key + " " + similarity);
					similarityMap.put(key.toString(), similarity);
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		try {
//			System.out.println("inside cleanup!");
			Double recommendationScore = 0.0;
//			System.out.println(similarityMap.size());
//			System.out.println(transitivityMap.size());
			for (Map.Entry<String, Double> entry : similarityMap.entrySet()) {
				String key = entry.getKey();
				Double similarity = entry.getValue();
//				System.out.println(key + ": " + transitivityMap.containsKey(key));
				if (transitivityMap.containsKey(key) && similarity > 0
						&& transitivityMap.get(key) > 0) {
					recommendationScore = similarity
							* (2 / transitivityMap.get(key));
					if(recommendationScore > 0){
						context.write(new Text(key), new Text(recommendationScore
								+ ""));
					}
					
//					System.out.println(transitivityMap.get(key));
//					System.out.println(key + "\t" + recommendationScore);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
