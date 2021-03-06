# Yelp-Friends-Recommendation
This project recommends a users other users based on collaborative filtering on business reviews and the trust relationship in the user's social graph.

## How to Run it
  * Install Maven
  * Install Eclipse IDE and import as maven project
  * Run command Maven Install
  * Copy the fat jar file from the target folder in classpath
  * Download Hadoop Binaries (version 2.0 to 2.6 works) and Setup hadoop distributed
  * Or replace the above step by running on EMR cluster by configuring as custom jar
  * run the command - **bin/hadoop jar /target/yelp_Recommendation-0.0.1-SNAPSHOT-jar-with-dependencies.jar business_data reviews_data users_data userID degree _(optional)_ outputfolderName _(optional)_**
  
##Results

Let's find out recommendations for Tina 

####Tina's reviewed businesses -
![alt text](https://github.com/bansalrachita/Yelp-Friends-Recommendation/blob/master/images/tina.png "Recommendation for user Tina")

####Recommended friends -
![alt text](https://github.com/bansalrachita/Yelp-Friends-Recommendation/blob/master/images/result.png "Recommendations")

####Graph Visualization
![alt text](https://github.com/bansalrachita/Yelp-Friends-Recommendation/blob/master/images/graph.png "graph visualization")



