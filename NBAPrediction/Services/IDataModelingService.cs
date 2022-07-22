using Microsoft.Spark.Sql;

namespace NBAPrediction.Services 
{
    internal interface IDataModelingService 
    {
        public void CreateNBADeltaTables(SparkSession spark);
    }

}