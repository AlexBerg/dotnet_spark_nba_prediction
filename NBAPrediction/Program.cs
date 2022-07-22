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

            var dataModeler = new DataModelingService(helper);

            dataModeler.CreateNBADeltaTables(spark);
        }
    }
}