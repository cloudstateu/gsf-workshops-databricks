# Partition the DataFrame by "year" and "month" and save it as a table in the schema
print(df.columns)
display(df)

df.write.format("delta").mode("overwrite").partitionBy("year", "month").saveAsTable("pentair_rest_workshops.tbl_sensor_data")

