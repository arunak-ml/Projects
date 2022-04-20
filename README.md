This particular code is solving the business problem of how much revenue is being generated from each search engine and which keywords are performing the best.

The input given is a hit level data which essentially consists of click stream hits that might have originated from either enterprise online web page click or offline data source or point of sales record that got uploaded which consists of different fields like ipaddress, geo location details, page url that the user hit, product list referrer and events .

Each ipaddress has multiple records which shows various stages of events like search , product view ,Add to shopping cart , shopping cart checkout and finally purchase.Revenue data is available when the user purchases the item which is purchase event, where as the search engine and keywords are available at search event or product view event.

Used pyspark code  inorder to handle huge volumes of data , this code initially accepts the hitlevel data as the input file and  derives the search engine and keyword fields from referrer url and subsequently fetches the revenue.

The resultant output file is being written to a output tsv file with a naming convention of [Date]_SearchKeywordPerformance where Date is the execution date of the program.

Executed this program in AWS using s3 and glue .

Created an s3 bucket and uploaded Input and searchengine_revenue.py file in s3 and created an IAM Role which allows s3 full access , glue full access, glue service policy to enable the cloudwatch logs.

![image](https://user-images.githubusercontent.com/71525207/164152542-c28cfc0f-4df8-409e-abab-d20fc223ff58.png)

![image](https://user-images.githubusercontent.com/71525207/164152639-955d35d8-12a3-45c6-820c-5ac46069f126.png)



Created a glue job(glue 2.0) with Spark 2.4 , Python 3 Version and provided the script location and executed the job , which created the output file in the provided s3 path.

![image](https://user-images.githubusercontent.com/71525207/164153238-8c26ae9b-704d-4ecc-bbac-332bacfb75ec.png)






