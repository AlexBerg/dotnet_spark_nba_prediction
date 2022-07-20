using Microsoft.Spark.Sql;
using NBAPrediction.Services;
using Microsoft.Spark.Extensions.Delta.Tables;
using Microsoft.Spark.Sql.Catalog;
using System;

namespace NBAPrediction
{
    class Program
    {
        static void Main(string[] args)
        {
            var helper = new HelperService();
            var spark = helper.GetSparkSession();
            var path = "/workspace/NBAPrediction/datasets/Advanced.csv";
            var df = helper.LoadFromCsv(spark, path);
            df.Show(numRows: 20);

            var db = spark.Catalog.CurrentDatabase();
            var dbs = spark.Catalog.ListDatabases();

            dbs.Show();

            if(!spark.Catalog.TableExists("advanced_stats"))
                df.Write().Format("delta").Mode("overwrite")
                    .Option("overwriteSchema", true)
                    .SaveAsTable("advanced_stats");
            else 
            {
                df = spark.Read().Table("advanced_stats");
                df.Show(numRows: 20, truncate: 0);
            }

            var tables = spark.Catalog.ListTables();

            tables.Show();
        }
    }
}