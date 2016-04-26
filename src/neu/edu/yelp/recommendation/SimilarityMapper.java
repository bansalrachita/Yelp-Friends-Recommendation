package neu.edu.yelp.recommendation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {

	Map<String, Double> personMap = new HashMap<>();
	Map<String, HashMap<String, Double>> usersMap = new HashMap<>();
	HashMap<String, Double> map = null;

	public void map(LongWritable key, Text value, Context context) {
		try {
			String line = value.toString();
			if (line != null && line.length() > 0) {
				String[] words = line.split(" ");

				if (words[0].equals(words[1])) {
					personMap.put(words[2], Double.parseDouble(words[3]));
				} else if (usersMap.containsKey(words[0] + " " + words[1])
						&& usersMap.get(words[0] + " " + words[1]) != null) {

					map = usersMap.get(words[0] + " " + words[1]);
					map.put(words[2], Double.parseDouble(words[3]));

				} else if (usersMap.containsKey(words[0] + " " + words[1])
						&& usersMap.get(words[0] + " " + words[1]) == null) {

					map = new HashMap<>();
					map.put(words[2], Double.parseDouble(words[3]));
					usersMap.put(words[0] + " " + words[1], map);

				} else if (!usersMap.containsKey(words[0] + " " + words[1])) {
					map = new HashMap<>();
					map.put(words[2], Double.parseDouble(words[3]));
					usersMap.put(words[0] + " " + words[1], map);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		try {

			System.out.println("inside cleanup!");
			// for (Map.Entry<String, Double> entry : personMap.entrySet()) {
			// mySimilaritySum += entry.getValue();
			// mySimilarityPower += Math.pow(entry.getValue(), 2);
			// }

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
