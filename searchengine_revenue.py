from pyspark.sql import SparkSession
from urllib.parse import urlparse
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
import re
import datetime
import boto3
import logging as log


class DomainKey():
    ##For Logging
    log.basicConfig()
    log.getLogger().setLevel(log.INFO)


    def renameS3File(s3, bckt_name, prefix, target_file):
        """
            This Function  renames the generated s3 output file with the given target naming convention.
            :param s3:   boto3 s3 client
            :param bckt_name: Aws s3 bucket name
            :param prefix: s3 sub folder where the file resides
            :param target_file: name of the target file in s3
            """
        try:
            response = s3.list_objects(
                Bucket=bckt_name,
                Prefix=prefix,
            )
            name = response["Contents"][0]["Key"]
            copy_source = {'Bucket': bckt_name, 'Key': name}

            s3.copy_object(Bucket=bckt_name, CopySource=copy_source, Key=target_file)
            s3.delete_object(Bucket=bckt_name, Key=name)
        except Exception as e:
            print("Error : ", str(e))

    # @udf(returnType=StringType())
    # def getkeyword(url):
    #     parsedUrl = urlparse(url)
    #     print(parsedUrl)
    #     netlocList = ['www.bing.com','www.google.com','search.yahoo.com']
    #     if parsedUrl[1] in netlocList :
    #         l1 = parsedUrl[4].split("&")
    #         matchRegex = re.compile('^[p|q]=[a-zA-Z]*')
    #         l2 = [ s for s in l1 if matchRegex.match(s) ]
    #         keyword = l2[0].split("=")[1].replace("+"," ")
    #         domain_nme = parsedUrl[1]
    #         return keyword + '_' + domain_nme
    #     else:
    #         return ''



    @udf(returnType=StringType())
    def getkeyword(url):
        """
             This UDF extracts the keywords and search engine from the referrer url
             :param url:  referrer url
             In referrer url, using both p & q query parameters to extract the keyword
             """
        try:
            parsedUrl = urlparse(url)
            domain_nme = parsedUrl[1]
            l1 = parsedUrl[4].split("&")
            matchRegex = re.compile('^[p|q]=[a-zA-Z]*')
            l2 = [s for s in l1 if matchRegex.match(s)]
            keyword = l2[0].split("=")[1].replace("+", " ")
            return keyword + '_' + domain_nme
        except:
            return ""


    @udf(returnType=StringType())
    def domainName(url):
        """
                     This UDF is used for extracting Domain name from url
                     :param url:  referrer url
                     """
        parsedUrl = urlparse(url)
        print(parsedUrl)
        return parsedUrl[1]


    #Initialize spark session
    spark = SparkSession.builder.appName('abc').getOrCreate()
    
    # used for target naming convention
    currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
    
    #Create s3 client to connect with s3
    s3 = boto3.client('s3')

    input_path = "s3://inputdata/Input_data1.csv" #parsing the input
    target_file = currentdate + "_SearchKeywordPerformance.csv"
    bckt_name = 'inputdata'
    prefix = 'outputfile/part-00'

    log.info("::::Reading Source file from s3 - {}".format(input_path))
    df = spark.read.csv(input_path, sep='\t', header=True)


    df1 = df.withColumn("Col1", \
                        when(((col("event_list").isNull()) | (col("event_list") == lit("2"))) & (
                                domainName(col("page_url")) != domainName(col("referrer"))) \
                             , getkeyword(col("referrer"))).otherwise(lit("")))

    df2 = df1.withColumn('Keyword', split(col("Col1"), '_').getItem(0)) \
        .withColumn('Domain', split(col("Col1"), '_').getItem(1)) \
        .select("ip", "Keyword", "Domain", "event_list", "pagename", "product_list")

    df2.createOrReplaceTempView("Tb1")

    df3 = spark.sql("""Select a.Keyword,a.Domain as Search_Engine,b.product_list 
                        from Tb1 a
                        Join Tb1 b On a.ip = b.ip 
                        where b.event_list = '1' and a.Keyword <> '' and a.Domain <>'' """) \
        .withColumn('revenue', split(col("product_list"), ';').getItem(3)) \
        .drop(col("product_list")).orderBy(col("revenue").desc())

    log.info("::::Writting csv file to s3 ")

    df3.coalesce(1).write.option('sep', '\t').option("header", "true").format("csv").mode("overwrite").save(
        "s3://inputdata/outputfile/")

    log.info("::::Renaming csv file to - {}".format(target_file))

    renameS3File(s3, bckt_name, prefix, target_file)