from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
from __future__ import print_function
import os

from boto.s3.connection import S3Connection
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import dayofmonth, unix_timestamp, concat, concat_ws
from datetime import datetime
from typing import List

# --packages org.apache.hadoop:hadoop-aws:3.1.1,org.apache.hadoop:hadoop-common:3.1.1
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages com.amazonaws:aws-java-sdk:1.11.390,org.apache.hadoop:hadoop-aws:3.1.1,"
    "org.apache.httpcomponents:httpcore:4.4.9 pyspark-shell "
)

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
basePath = "s3a://magnet-fwm/home/TsviKuflik/"

def spark_initiator():
    # type: () -> SparkContext
    conf = SparkConf().setAppName("yuri")
    sc = SparkContext.getOrCreate(conf)

    sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

    sc._jsc.hadoopConfiguration().set("spark.dynamicAllocation.enabled", "true")
    sc._jsc.hadoopConfiguration().set("spark.shuffle.service.enabled", "true")
    return sc


sc = spark_initiator()  # type: SparkContext

print("\n" + "#" * 70 + " Genres distribution " + "#" * 70 + "\n")

format = "yyyyMMddHHmmss"
limitDatePickViews = datetime(2015, 6, 1)  # type: datetime
cities = ["Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma"]
dates = "0601-0608"
print("cities=" + str(cities))
print("dates=" + dates)
programs_path = basePath + "y_programs/programs_genres_groups/part-00000"


# <editor-fold desc="Functions">

def delete_if_exists(path):
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI("s3n://magnet-fwm"), sc._jsc.hadoopConfiguration())

    if fs.exists(Path(path)):
        print("Deleting " + path)
        fs.delete(Path(path))


def programs_to_genres(s):
    sp = s.split("|")
    genres = sp[1].split(",")
    codes = sp[2:]
    return [(c, genres) for c in codes]


def views_to_genres_list(s):
    sp = s.split(",")
    prog_codes = [view.split("|")[1] for view in sp[1:]]
    genres_lists = [programs_code_to_genres[code] for code in prog_codes if code]
    genres = [val for sublist in genres_lists for val in sublist]
    return genres


# </editor-fold>

programs_code_to_genres = dict(sc.textFile(programs_path) \
                               .flatMap(lambda s: programs_to_genres(s)) \
                               .collect())

views_paths = [basePath + "y_for_prediction/views/5_months/" + city.replace(" ", "-") + "-" + dates + "/*.gz" for city
               in cities]  # type: List[str]

print(",".join(views_paths))

views_genres_dist = sc.textFile(",".join(views_paths)) \
    .flatMap(lambda v: views_to_genres_list(v)) \
    .map(lambda v: (v, 1)) \
    .reduceByKey(lambda _1, _2: _1 + _2) \
    .sortBy(lambda g: -g[1]) \
    .map(lambda g: g[0] + "," + str(g[1]))

dest_path = basePath + "y_for_prediction/views/5_months/genres-dist/" + dates + "/"

delete_if_exists(dest_path)

views_genres_dist.coalesce(1).saveAsTextFile(dest_path)

print("Done!")
