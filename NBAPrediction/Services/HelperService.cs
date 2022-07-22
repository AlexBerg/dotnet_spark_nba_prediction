using Microsoft.Spark.Sql;

namespace NBAPrediction.Services
{
    internal class HelperService : IHelperService
    {

        public SparkSession GetSparkSession()
        {
            return SparkSession.Builder()
                .EnableHiveSupport()
                .AppName("dotnet_spark_nba_predicition")
                .Config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .Config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .GetOrCreate();
        }

        public DataFrame LoadFromCsv(SparkSession spark, string path)
        {
            return spark.Read()
                .Format("csv")
                .Option("sep", ",")
                .Option("header", true)
                .Option("inferSchema", true)
                .Load(path);
        }

        public void SaveAsManagedDeltaTable(DataFrame dataFrame, string tableName)
        {
            dataFrame.Write()
                .Format("delta")
                .Mode(SaveMode.Overwrite)
                .Option("overwriteSchema", true)
                .SaveAsTable(tableName);
        }

        public DataFrame LoadFromManagedDeltaTable(SparkSession spark, string tableName) 
        {
            return spark.Read()
                .Format("delta")
                .Table(tableName);
        }
    }
}