using Microsoft.Spark.Sql;
using NBAPrediction.Services;

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

            df.Write().Format("delta").Mode("overwrite")
                .Option("overwriteSchema", true)
                .Save("/workspace/NBAPrediction/datasets/delta/advanced");

            df = spark.Read().Format("delta").Load("/workspace/NBAPrediction/datasets/delta/advanced");

            df.Show(numRows: 20);
        }
    }
}