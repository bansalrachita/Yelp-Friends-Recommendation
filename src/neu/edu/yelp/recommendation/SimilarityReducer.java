package neu.edu.yelp.recommendation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, Text, Text, Text> {

	Map<String, Double> personMap = new HashMap<>();
	Map<String, HashMap<String, Double>> usersMap = new HashMap<>();
	HashMap<String, Double> map = null;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text line : values) {
			// Text word = new Text();
			String[] words = line.toString().split(" ");
			System.out.println(line.toString());
			if (key.toString().split(" ")[0]
					.equals(key.toString().split(" ")[1])) {
				System.out.println("inside return " +  key.toString());
				return;
			}

			if (personMap != null && !personMap.containsKey(words[0])) {
				personMap.put(words[0], Double.parseDouble(words[1]));
			}

			if (usersMap.containsKey(key.toString())
					&& usersMap.get(key.toString()) != null) {

				map = usersMap.get(key.toString());
				map.put(words[0], Double.parseDouble(words[2]));

			} else if (usersMap.containsKey(key.toString())
					&& usersMap.get(key.toString()) == null) {

				map = new HashMap<>();
				map.put(words[0], Double.parseDouble(words[2]));
				usersMap.put(key.toString(), map);

			} else if (!usersMap.containsKey(key.toString())) {
				map = new HashMap<>();
				map.put(words[0], Double.parseDouble(words[2]));
				usersMap.put(key.toString(), map);
			}
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		try {
			for (Map.Entry<String, HashMap<String, Double>> entry : usersMap
					.entrySet()) {
				String userIds = entry.getKey();
				int count = 0;
				double mySimilaritySum = 0;
				double otherSimilaritySum = 0;
				double mySimilarityPower = 0;
				double otherSimilarityPower = 0;
				double sumOfProducts = 0;
				double pearsonScore = 0;

				for (Map.Entry<String, Double> entryItems : entry.getValue()
						.entrySet()) {
					if (personMap.containsKey(entryItems.getKey())) {
						count++;
						mySimilaritySum += personMap.get(entryItems.getKey());
						mySimilarityPower += Math.pow(
								personMap.get(entryItems.getKey()), 2);

						otherSimilaritySum += entryItems.getValue();
						otherSimilarityPower += Math.pow(entryItems.getValue(),
								2);
						System.out.println(personMap.get(entryItems.getKey()
								.toString()) + " " + entryItems.getValue());
						sumOfProducts += personMap.get(entryItems.getKey())
								* entryItems.getValue();
					} else {
						System.out.println("key : " + entry.getKey()
								+ "value : " + entry.getValue());
					}

				}
				if (count == 0) {
					continue;
				}

				double num = sumOfProducts
						- (mySimilaritySum * otherSimilaritySum / count);

				double den = Math.sqrt((mySimilarityPower - Math.pow(
						mySimilaritySum, 2) / count)
						* (otherSimilarityPower - Math.pow(otherSimilaritySum,
								2) / count));
				if (den == 0) {
					continue;
				}

				pearsonScore = num / den;

				context.write(new Text(userIds), new Text(pearsonScore + ""));

				System.out.println("mySimilaritySum : " + mySimilaritySum
						+ "otherSimilaritySum : " + otherSimilaritySum
						+ "mySimilarityPower : " + mySimilarityPower
						+ "otherSimilarityPower: " + otherSimilarityPower
						+ "sumOfProducts :" + sumOfProducts + "count: " + count
						+ "pearsonScore : " + pearsonScore);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
