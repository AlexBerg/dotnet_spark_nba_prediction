using Microsoft.Spark.Sql;

namespace NBAPrediction.Services
{
    internal class HelperService : IHelperService {
        
        public string GetCorrectFilePath(string path) {
            throw new System.NotImplementedException();
        }

        public SparkSession GetSparkSession() {
            throw new System.NotImplementedException();
        }

        public DataFrame LoadFromCsv(string path) {
            throw new System.NotImplementedException();
        }
    }
}