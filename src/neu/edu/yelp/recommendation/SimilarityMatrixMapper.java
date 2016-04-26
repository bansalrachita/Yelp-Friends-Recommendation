//package neu.edu.yelp.recommendation;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import com.google.gson.Gson;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//
//public class SimilarityMatrixMapper extends
//		Mapper<LongWritable, Text, Text, Text> {
//
//	Gson gson = new Gson();
//
//	public void map(LongWritable key, Text value, Context context) {
//		try {
//			String line = value.toString();
//			if (line != null && line.length() > 0) {
//				// System.out.println(line);
//				JsonObject jsonObject = gson.fromJson(line, JsonObject.class);
//				JsonElement jsonObjType = jsonObject.get("type");
//				JsonElement jsonObjUserId = jsonObject.get("user_id");
//
//				if (null != jsonObjType
//						&& jsonObjType.toString().equals("\"review\"")) {
//					JsonElement jsonObjBusinessId = jsonObject
//							.get("business_id");
//					JsonElement jsonObjStars = jsonObject.get("stars");
//					context.write(
//							new Text(jsonObjUserId.toString().substring(1,
//									jsonObjUserId.toString().length() - 1)),
//							new Text(jsonObjBusinessId.toString().substring(1,
//									jsonObjBusinessId.toString().length() - 1)
//									+ "," + jsonObjStars.toString()));
//					// System.out.println(jsonObjUserId.toString().substring(1,
//					// jsonObjUserId.toString().length() - 1) + "\t" +
//					// jsonObjBusinessId.toString().substring(1,
//					// jsonObjBusinessId.toString().length() - 1)
//					// + "," + jsonObjStars.toString());
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	// public static void main(String[] args) {
//	// Gson gson = new Gson();
//	// String jsonString =
//	// "{\"yelping_since\": \"2004-10\"	, \"votes\": {\"funny\": 6, \"useful\": 30, \"cool\": 15}, \"review_count\": 20, \"name\": \"Joseph\", \"user_id\": \"qL7Astun3i7qwr2IL5iowA\", \"friends\": [\"4U9kSBLuBDU391x6bxU-YA\", \"PMgE5Yqv7QL_cTjjInyYIg\", \"NMKADwkprjoc1nc8AuEAgg\", \"KKmpVu4m8VwjnPaAqxTufg\", \"SqNfdBybcOTe6oaLmHxnLQ\", \"NADH23dCqDgcIczNEnKPkQ\"], \"fans\": 0, \"average_stars\": 4.3, \"type\": \"user\", \"compliments\": {\"photos\": 1, \"hot\": 1, \"cool\": 3, \"plain\": 2}, \"elite\": []}";
//	// JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
//	// System.err.println("gson back " + jsonObject.get("yelping_since"));
//	// JsonElement jsonObject2 = jsonObject.get("type");
//	// System.err.println("gson back " + jsonObject2);
//	// System.out.println("=> " + gson);
//	// }
//
//}
