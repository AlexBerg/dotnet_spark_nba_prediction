using Microsoft.Spark.Sql;
using Microsoft.ML;
using Microsoft.Data.Analysis;
using Microsoft.ML.Trainers;
using System.Collections.Generic;
using System;

using F = Microsoft.Spark.Sql.Functions;
using T = Microsoft.Spark.Sql.Types;
using D = Microsoft.ML.Data.DataKind;
using ML = Microsoft.ML;
using Spark = Microsoft.Spark;

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
                var cols = CreateMVPAwardShareWithStatsDataSet(spark);

                var mlContext = new MLContext();

                var model = TrainModel(mlContext, cols);

                EvaluateModel(mlContext, model, cols);
            }
            catch (System.Exception)
            {
                CleanupTempFiles();
                throw;
            }

            CleanupTempFiles();
        }

        private ITransformer TrainModel(MLContext mlContext, ML.Data.TextLoader.Column[] cols)
        {
            var trainingData = LoadFromCsvFile(mlContext, "datasets/temp/mvp/training/*.csv", cols);

            var pipeline = mlContext.Transforms
                .Concatenate("Features",
                    "ValueOverReplacementPlayer", "PlayerEfficiencyRating", "WinShares", "TrueShootingPercentage",
                    "TotalReboundPercentage", "AssistPercentage", "StealPercentage", "BlockPercentage", "TurnoverPercentage", "OnCourtPlusMinusPer100Poss", "PointsGeneratedByAssitsPerGame")
                .Append(mlContext.Regression.Trainers.FastForest(labelColumnName: "Share", featureColumnName: "Features"));

            var model = pipeline.Fit(trainingData);

            return model;
        }

        private void EvaluateModel(MLContext mlContext, ITransformer model, ML.Data.TextLoader.Column[] cols)
        {
            var testData = LoadFromCsvFile(mlContext, "datasets/temp/mvp/test/*.csv", cols);

            var predictions = model.Transform(testData);

            var metrics = mlContext.Regression.Evaluate(predictions, "Share", "Score");
        }

        private IDataView LoadFromCsvFile(MLContext mlContext, string path, ML.Data.TextLoader.Column[] cols)
        {
            var dw = mlContext.Data.LoadFromTextFile(path, cols, separatorChar: ',', hasHeader: true);

            return dw;
        }

        private ML.Data.TextLoader.Column[] CreateMVPAwardShareWithStatsDataSet(SparkSession spark)
        {
            var mvpAwardShareWithStats = spark.Sql(
                    @"SELECT pt.*, a.Share, a.Award, a.WonAward FROM (
                        SELECT p.*, t.GamesPlayed AS TeamGamesPlayed, t.League, ROUND(t.Wins / t.GamesPlayed, 2) AS WinPercentage, t.AverageMarginOfVictory, t.NetRating FROM
                        (
                            SELECT past.*, pst.PointsPerGame, pst.GamesPlayed, pst.GamesStarted, pst.MinutesPerGame, pbp.OnCourtPlusMinusPer100Poss, pbp.NetPlusMinutPer100Poss, pbp.PointsGeneratedByAssitsPerGame FROM 
                            PlayerSeasonStats AS pst
                            LEFT JOIN PlayerSeasonAdvancedStats AS past ON pst.PlayerId = past.PlayerId AND pst.Season = past.Season AND pst.TeamId = past.TeamId
                            LEFT JOIN PlayerSeasonPlayByPlayStats AS pbp ON pst.PlayerId = pbp.PlayerId AND pst.Season = pbp.Season AND pst.TeamId = pbp.TeamId
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
                AND GamesPlayed / TeamGamesPlayed >= 0.50
                AND League = 'NBA'")
            .WithColumn("Share", F.When(F.Col("Award") == "nba mvp", F.Col("Share")).Otherwise(null))
            .Na().Fill(new Dictionary<string, double>() { { "Share", 0.0 } })
            .Na().Fill(new Dictionary<string, bool>() { { "WonAward", false } });

            var trainingData = mvpAwardShareWithStats.Filter("Season != 2022");
            var testData = mvpAwardShareWithStats.Filter("Season = 2022");

            trainingData.Write().Format("csv").Option("header", true)
                .Save("/workspace/NBAPrediction/datasets/temp/mvp/training");

            testData.Write().Format("csv").Option("header", true)
                .Save("/workspace/NBAPrediction/datasets/temp/mvp/test");

            var cols = GetColumns(mvpAwardShareWithStats);

            return cols;
        }

        private void CleanupTempFiles()
        {
            if (System.IO.Directory.Exists("datasets/temp"))
                System.IO.Directory.Delete("datasets/temp", true);
        }

        private ML.Data.TextLoader.Column[] GetColumns(Spark.Sql.DataFrame df)
        {
            var resultList = new List<ML.Data.TextLoader.Column>();
            var schema = df.Schema();
            var index = 0;
            foreach (var field in schema.Fields)
            {
                var fieldType = field.DataType.GetType();
                var type = fieldType == typeof(T.BooleanType) ? D.Boolean :
                    fieldType == typeof(T.StringType) ? D.String : D.Single;
                var col = new ML.Data.TextLoader.Column(field.Name, type, index);
                resultList.Add(col);
                index += 1;
            }

            return resultList.ToArray();
        }
    }
}