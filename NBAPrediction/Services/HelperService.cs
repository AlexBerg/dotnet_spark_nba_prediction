using Microsoft.Spark.Sql;
using System.Collections.Generic;

namespace NBAPrediction.Services
{
    internal class HelperService : IHelperService {

        public SparkSession GetSparkSession() {
            return SparkSession.Builder()
                .EnableHiveSupport()
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

        public bool SaveAsManagedDeltaTable(SparkSession spark, DataFrame dataFrame, string tableName, Dictionary<string, string> options) 
        {
            if(!spark.Catalog.TableExists(tableName)) 
            {
                dataFrame.Write()
                    .Format("delta")
                    .SaveAsTable(tableName);
            }
            else 
            {
                dataFrame.Write()
                    .Format("delta")
                    .Options(options)
                    .SaveAsTable(tableName);
            }

            return true;
        }
    }
}