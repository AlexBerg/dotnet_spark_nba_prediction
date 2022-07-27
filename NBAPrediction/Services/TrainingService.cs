using Microsoft.Spark.Sql;
using Microsoft.ML;
using NBAPrediction.DataTypes;
using Microsoft.Data.Analysis;
using Microsoft.ML.Trainers;
using System.Collections.Generic;

using F = Microsoft.Spark.Sql.Functions;
using T = Microsoft.Spark.Sql.Types;

namespace NBAPrediction.Services
{
    internal class TrainingService : ITrainingService
    {
        private readonly IHelperService _helperService;

        public TrainingService(IHelperService helperService)
        {
            _helperService = helperService;
        }

        public void TrainAndEvaluateMVPPredicitionModel(SparkSession spark)
        {
            try
            {
                CreateMVPAwardShareWithStatsDataSet(spark);

                var mlContext = new MLContext();

                var model = TrainModel(mlContext);

                EvaluateModel(mlContext, model);
            }
            catch (System.Exception)
            {
                CleanupTempFiles();
                throw;
            }

            CleanupTempFiles();
        }

        private ITransformer TrainModel(MLContext mlContext)
        {
            var trainingData = mlContext.Data.LoadFromTextFile<MVPWithStatsData>("datasets/temp/mvp/training/*.csv", separatorChar: ',', hasHeader: true);

            var pipeline = mlContext.Transforms
                .Concatenate("Features", 
                    "ValueOverReplacementPlayer", "BoxPlusMinus", "PlayerEfficiencyRating", "WinShares", "GamesStarted", "PointsPerGame")
                .Append(mlContext.Regression.Trainers.FastForest(labelColumnName: "Share", featureColumnName: "Features"));

            var model = pipeline.Fit(trainingData);

            return model;
        }

        private void EvaluateModel(MLContext mlContext, ITransformer model)
        {
            var testData = mlContext.Data.LoadFromTextFile<MVPWithStatsData>("datasets/temp/mvp/test/*.csv", separatorChar: ',', hasHeader: true);

            var predictions = model.Transform(testData);

            var metrics = mlContext.Regression.Evaluate(predictions, "Share", "Score");
        }

        private void CreateMVPAwardShareWithStatsDataSet(SparkSession spark)
        {
            var mvpAwardShareWithStats = spark.Sql(
                    @"SELECT pt.*, a.Share, a.Award, a.WonAward FROM (
                        SELECT p.*, t.GamesPlayed as TeamGamesPlayed, t.League FROM
                        (
                            SELECT past.*, pst.PointsPerGame, pst.GamesPlayed, pst.GamesStarted, pst.MinutesPerGame FROM 
                            PlayerSeasonStats AS pst
                            LEFT JOIN PlayerSeasonAdvancedStats AS past ON pst.PlayerId = past.PlayerId AND pst.Season = past.Season AND pst.TeamId = past.TeamId
                        ) AS p
                        LEFT JOIN (
                            SELECT tst.*, team.League FROM
                            TeamSeasonStats AS tst
                            LEFT JOIN Teams as team ON tst.TeamId = team.TeamId 
                        ) AS t
                        ON p.TeamId = t.TeamId AND p.Season = t.Season
                        ) as pt
                        LEFT JOIN PlayerSeasonAwardShare AS a ON pt.PlayerId = a.PlayerId AND pt.Season = a.Season AND pt.TeamId = a.TeamId"
            ).Filter(@"MinutesPerGame >= 20.0 
                AND GamesStarted / GamesPlayed >= 0.50 
                AND GamesPlayed / TeamGamesPlayed > 0.50
                AND League = 'NBA'")
            .WithColumn("Share", F.When(F.Col("Award") == "nba mvp", F.Col("Share")).Otherwise(null))
            .Na().Fill(new Dictionary<string, double>() { {"Share", 0.0} })
            .Na().Fill(new Dictionary<string, bool>() { {"WonAward", false} });

            var trainingData = mvpAwardShareWithStats.Filter("Season != 2021");
            var testData = mvpAwardShareWithStats.Filter("Season = 2021");

            trainingData.Write().Format("csv").Option("header", true)
                .Save("/workspace/NBAPrediction/datasets/temp/mvp/training");

            testData.Write().Format("csv").Option("header", true)
                .Save("/workspace/NBAPrediction/datasets/temp/mvp/test");
        }

        private void CleanupTempFiles()
        {
            if(System.IO.Directory.Exists("datasets/temp"))
                System.IO.Directory.Delete("datasets/temp", true);
        }
    }
}