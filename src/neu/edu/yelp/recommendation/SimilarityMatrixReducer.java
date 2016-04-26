//package neu.edu.yelp.recommendation;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//public class SimilarityMatrixReducer extends Reducer<Text, Text, Text, Text> {
//
//	Map<String, String> criticsMap = new HashMap<>();
//	String person = null;
//
//	@Override
//	protected void setup(Context context) throws IOException,
//			InterruptedException {
//		person = context.getConfiguration().get("person",
//				"qL7Astun3i7qwr2IL5iowA");
//		System.out.println("Reduce setup called! " + person);
//	}
//
//	public void reduce(Text key, Iterable<Text> values, Context context) {
//		for (Text line : values) {
//			if (criticsMap.containsKey(key.toString())) {
//				criticsMap.put(key.toString(), criticsMap.get(key.toString())
//						+ ":" + line.toString());
//			} else {
//				criticsMap.put(key.toString(), line.toString());
//			}
//		}
//	}
//
//	@Override
//	protected void cleanup(Context context) throws IOException,
//			InterruptedException {
//		try {
//			for (Map.Entry<String, String> entry : criticsMap.entrySet()) {
//				if (entry.getKey().equals(person)) {
//					continue;
//				}
//				// System.out.println("map entry : " + entry.getKey());
//				double sim = similarityMatrix(person, entry.getKey().toString());
//				// System.out.println(person + "\t" + sim);
//				context.write(new Text(person), new Text(entry.getKey()
//						.toString() + " " + sim));
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	public double similarityMatrix(String person, String other) {
//		Map<String, Double> myBusinessList = new HashMap<>();
//		double mySimilaritySum = 0;
//		double otherSimilaritySum = 0;
//		double mySimilarityPower = 0;
//		double otherSimilarityPower = 0;
//		double sumOfProducts = 0;
//		double pearsonScore = 0;
//		int count = 0;
//
//		// System.out.println("temp : ");
//		for (String temp : criticsMap.get(person).split(":")) {
//			myBusinessList.put(temp.split(",")[0],
//					Double.parseDouble(temp.split(",")[1]));
//		}
//
//		Map<String, Double> otherBusinessList = new HashMap<>();
//
//		// System.out.println("temp1 : ");
//		for (String temp1 : criticsMap.get(other).split(":")) {
//			otherBusinessList.put(temp1.split(",")[0],
//					Double.parseDouble(temp1.split(",")[1]));
//		}
//
//		for (Map.Entry<String, Double> entry : myBusinessList.entrySet()) {
//			String key = entry.getKey();
//			Double value = entry.getValue();
//
//			// System.out.println("key : " + key + " value :" + value);
//
//			if (otherBusinessList.containsKey(key)) {
//				count++;
//				mySimilaritySum += value;
//				otherSimilaritySum += otherBusinessList.get(key);
//				mySimilarityPower += Math.pow(value, 2);
//				otherSimilarityPower += Math.pow(otherBusinessList.get(key), 2);
//				sumOfProducts += value * otherBusinessList.get(key);
//			}else{
//				System.out.println("key : " + entry.getKey() + "value : " + entry.getValue() );
//			}
//
//		}
//		 System.out.println("mySimilaritySum : " + mySimilaritySum
//		 + "otherSimilaritySum : " + otherSimilaritySum
//		 + "mySimilarityPower : " + mySimilarityPower
//		 + "otherSimilarityPower: " + otherSimilarityPower
//		 + "sumOfProducts :" + sumOfProducts);
//
//		// pearson score
//		
//		if (count == 0){
//			return 0;
//		}
//			
//
//		double num = sumOfProducts
//				- (mySimilaritySum * otherSimilaritySum / count);
//		double den = Math.sqrt((mySimilarityPower - Math
//				.pow(mySimilaritySum, 2) / count)
//				* (otherSimilarityPower - Math.pow(otherSimilaritySum, 2)
//						/ count));
//		// System.out.println("num : " + num + "den : " + den);
//		if (den == 0)
//			return 0;
//		pearsonScore = num / den;
//		return pearsonScore;
//	}
//}
