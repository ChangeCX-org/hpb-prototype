'''The goal of this script is to rank simiarity of titles using Spacy and NLP techniques
For each title entry the other titles having same author / authors are taken and compared. 
The resulting score is assigned to other titles. Once we have a higher propensity of scores we can group the related
titles to primary title.
'''
import time
import pyspark.sql.functions as F
import spacy
import en_core_web_lg
import pandas as pd
import numpy as np
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,count
from pyspark.sql.types import FloatType,StructField, StructType, StringType, MapType
from pyspark.sql.types import array

class TitlesGrouper:
    
    @staticmethod
    def get_spacy():

        if "nlp" not in globals():
            globals()["nlp"] = en_core_web_lg.load()

        return globals()["nlp"]
    

    @staticmethod
    def get_sparkdf():        
        if "sparkdf" not in globals():
            spark = SparkSession.builder.master("local[1]").appName("SparkSamples").getOrCreate()            
            globals()["spark"] = spark
            globals()["sparkdf"] = spark.read.option("header",True)\
                                    .csv("curated_book_short.csv")
        return globals()["sparkdf"]            

    @staticmethod
    def get_spark():
        if "spark" not in globals():
            spark = SparkSession.builder.master("local[1]").appName("SparkSamples").getOrCreate()            
            globals()["spark"] = spark
            return globals()["spark"]
        else:
            return globals()["spark"]
    
    @staticmethod
    def text_similarity(dsc1, dsc2):
        '''
        Load spacy and calculate similarity measure
        '''
        if (dsc1 is None or dsc2 is None):
            return float(-1)
        if ((len(str(dsc1)) < 1 or str(dsc1) == "") or (len(str(dsc2)) < 1 or str(dsc2) == "")):
            return float(-1)
        nlp_glob = TitlesGrouper.get_spacy()
        return float(nlp_glob(dsc1).similarity(nlp_glob(dsc2)))        

    #define your udf 

start = time.process_time()
text_similarity_UDF = udf(lambda arr: TitlesGrouper.text_similarity(arr[0], arr[1]), FloatType())        
text_similarity_UDF_linear = udf(TitlesGrouper.text_similarity, FloatType())        
TitlesGrouper.get_spark().udf.register("text_similarity_UDF_linear",TitlesGrouper.text_similarity,FloatType())
title1_df = TitlesGrouper.get_sparkdf()
title2_df = TitlesGrouper.get_sparkdf()
#title1_df.groupBy("author").count().show(100,truncate=False)

#titleResults1 = title1_df.select("title").show(10)
#titleResults2 = title1_df.select("title").show(100)
#text_similarity_with_measure =  title1_df.\
                                #select("title","title",\
                                #text_similarity_UDF1("title","title").alias("Similarity_Score"))


'''
The logic of comparison goes like this
For each title in title2_df get list of titles from title_df_as_array where
title2_df.author in (title_df_as_array.author)
Compare title2_df.title's similarity to title_df_as_array.title
If the Simiarity score > 0.75 then update title2_df.id:title2_df.title:title2_df.author:title
The reason for 75% is based on a small test refer to TitleSimilarity.py
'''
title1_df.createOrReplaceTempView("title_source")

authorwise_titlecountdf = TitlesGrouper.get_spark().sql("SELECT t.author as source_author,\
                            t.title as source_title, count(t.id) as title_count \
                            from title_source t \
                            group by t.author,t.title order by title_count desc, t.author asc")

authorwise_titlecountdf.show(100,False)                    

print("Count of titles are :",authorwise_titlecountdf.count())
authorwise_titlecountdf.createOrReplaceTempView("Authorwise_TitleCount")
title_df_as_array = authorwise_titlecountdf.withColumn("SimilarTitles_over_77Percent",F.array())
title_df_as_array.printSchema()
title_df_as_array.createOrReplaceTempView("title_with_similarities_over77pct")
df_77percent = TitlesGrouper.get_spark().sql("select source.source_title,source.source_author,77pct.source_title as similar_title,\
                            text_similarity_UDF_linear(source.source_title,77pct.source_title)*100 as similarity_score \
                            from Authorwise_TitleCount source,title_with_similarities_over77pct 77pct \
                            where source.source_author=77pct.source_author \
                            and source.source_title != 77pct.source_title")

df_77percent = df_77percent.where(df_77percent.similarity_score > 77)
df_77percent.show(50,False)
df_77percent.createOrReplaceTempView("Similarity_For_Titles")
df_77percent.coalesce(1).write.csv('Similarity_For_Titles_Over77Pct.csv')
#final_df = TitlesGrouper.get_spark().sql("select source_title,source_author,similarity_score,count(similar_title) as similar_title_count \
 #                                       from Similarity_For_Titles \
  #                                      group by \
   #                                     source_title,source_author,similarity_score \
    #                                    order by similar_title_count, similarity_score desc")
#final_df.show(50,False)
#print("The total count of consolidated titles are :",final_df.count())
#TitlesGrouper.get_spark().sql("SELECT t.title as source_title \
 #                   from title_source t \
  #                  where t.author = 'George Eliot'").show(100,False)

#text_similarity_with_measure =  title1_df.join(title2_df,title1_df("author")==title2_df("author")).\
 #                   select(title1_df("title").alias("title1"),title2_df("title").alias("title2"),\
  #                  text_similarity_UDF1("title1","title2").alias("Similarity_Score"))

#print("The text Similarity with Measure is ",text_similarity_with_measure.show(100))
print(time.process_time() - start)
