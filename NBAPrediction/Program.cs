using Microsoft.Spark.Sql;

namespace NBAPrediction
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var path = "datasets/Advanced.csv";
            var df = spark.Read()
                .Format("csv")
                .Option("sep", ",")
                .Option("header", true)
                .Option("inferSchema", true)
                .Load(path);
            df.Show(numRows: 20);
        }
    }
}