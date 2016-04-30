data = read.csv("/home/rachita/Desktop/mr_final_project/reviewsPerUser.csv", header = TRUE)
png(filename = "ReviewsPerUser.png")
hist(data$Reviews, 
     main="Histogram for User Reviews", 
     xlab="Reviews",ylab ="User Count", 
     border="black", 
     col="blue",xlim = c(1,2000),
     ylim = c(1, 100),
     las=1, 
     breaks=5)
legend("topright",
       bg = "wheat", 
       fill="blue", 
       legend=c("0-500: Users>1k", 
                "500-1K: 0<Users<40",
                "1K-1.5K: Users<10"))
dev.off()

png(filename = "ReviewsPerUser.png")
dataS = data[data$Reviews<500, ]
hist(dataS$Reviews, 
     main="Histogram for User Reviews", 
     xlab="Reviews",ylab ="User Count", 
     border="black", 
     col="blue",xlim = c(1,500),
     ylim = c(1, 2000),
     las=1, 
     breaks=5)
legend("topright",
       bg = "wheat", 
       fill="blue", 
       legend=c("0-100 Reviews: Users>1k", 
                ">200 Reviews: 0<Users<1K"
       ))
dev.off()
data = read.csv("/home/rachita/Desktop/mr_final_project/business.csv", header = TRUE)
png(filename = "ReviewsPerBusiness.png")
hist(data$Reviews, 
     main="Histogram for Business Reviews", 
     xlab="Reviews",ylab ="Business Count", 
     border="black", 
     col="blue",xlim = c(1,5000),
     ylim = c(1, 1000),
     las=1, 
     breaks=5)
legend("topright",
       bg = "wheat", 
       fill="blue", 
       legend=c("0<Reviews<1k: Business>100", "1k< Reviews<2k: 0<Business<80", "2k< Reviews<3k: 0<Business<40","3k< Reviews<4k: 0<Business<20"
       ))
dev.off()