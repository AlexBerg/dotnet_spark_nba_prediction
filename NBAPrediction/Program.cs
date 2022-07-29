﻿using Microsoft.Spark.Sql;
using NBAPrediction.Services;


namespace NBAPrediction
{
    class Program
    {
        static void Main(string[] args)
        {
            var helper = new HelperService();
            var spark = helper.GetSparkSession();

            var training = new TrainingService(helper);

            var dataModelingService = new DataModelingService(helper);

            training.TrainAndEvaluateMVPPredicitionModel(spark);
        }
    }
}