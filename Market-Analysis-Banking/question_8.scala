val df = spark.read.format("csv").option("header","true").option("inferschema","true").option("delimiter",",")
.load("/user/maew711gmail/data/marketing-analysis-dataset.csv")

val df_new = df.withColumn("18_to_29", when($"age" < 30, 1).otherwise(0))
.withColumn("30_to_40", when($"age" > 29 && $"age" < 41, 1).otherwise(0))
.withColumn("40_to_50", when($"age" > 39 && $"age" < 51, 1).otherwise(0))
.withColumn("Over_50", when($"age" > 49, 1).otherwise(0))

df_new.filter($"y"==="yes").agg(sum("18_to_29").as("18_to_29"),sum("30_to_40").as("30_to_40"),sum("40_to_50").as("40_to_50"),sum("Over_50").as("Over_50")).show()


