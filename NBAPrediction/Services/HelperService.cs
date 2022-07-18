using Microsoft.Spark.Sql;

namespace NBAPrediction.Services
{
    internal class HelperService : IHelperService {
        
        public string GetCorrectFilePath(string path) {
            throw new System.NotImplementedException();
        }

        public SparkSession GetSparkSession() {
            return SparkSession.Builder()
                .AppName("dotnet_spark_nba_predicition")
                .Config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .Config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .GetOrCreate();
        }

        public DataFrame LoadFromCsv(SparkSession spark, string path) {
            return spark.Read()
                .Format("csv")
                .Option("sep", ",")
                .Option("header", true)
                .Option("inferSchema", true)
                .Load(path);
        }
    }
}