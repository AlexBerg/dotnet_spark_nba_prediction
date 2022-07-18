using Microsoft.Spark.Sql;

namespace NBAPrediction.Services
{
    internal interface IHelperService
    {
        public string GetCorrectFilePath(string path);
        public SparkSession GetSparkSession();
        public DataFrame LoadFromCsv(SparkSession spark, string path);
    }
}
