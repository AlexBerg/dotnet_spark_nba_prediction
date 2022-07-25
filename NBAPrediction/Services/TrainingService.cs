using Microsoft.Spark.Sql;
using Microsoft.ML;
using NBAPrediction.DataTypes;

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

        public void TrainMVPPredicitionModel(SparkSession spark) 
        {
            CreateMVPAwardShareWithStatsDataSet(spark);

            var mlContext = new MLContext();            

            var data = mlContext.Data.LoadFromTextFile<MVPWithStatsData>("datasets/temp/mvpwithstats/*.csv", separatorChar: ',', hasHeader: true);

            CleanupTempFiles();
        }

        private void CreateMVPAwardShareWithStatsDataSet(SparkSession spark) 
        {
            var mvpAwardShareWithStats = spark.Sql(
                @"SELECT past.*, a.Share, a.WonAward, pst.PointsPerGame, pst.GamesPlayed, pst.GamesStarted FROM
                    PlayerSeasonAwardShare AS a
                    JOIN PlayerSeasonStats AS pst ON a.PlayerId = pst.PlayerId AND a.Season = pst.Season
                    JOIN PlayerSeasonAdvancedStats AS past ON a.PlayerId = past.PlayerId AND a.Season = past.Season
                    JOIN TeamSeasonStats AS tst ON a.TeamId = tst.TeamId AND a.Season = tst.Season
                    JOIN Teams AS t ON a.TeamId = t.TeamId
                    WHERE 
                        t.League = 'NBA' AND 
                        a.Award = 'nba mvp' AND 
                        pst.GamesPlayed / tst.GamesPlayed >= 0.50 AND 
                        pst.GamesStarted / pst.GamesPlayed >= 0.50 AND
                        pst.MinutesPerGame >= 20.0 AND 
                        tst.MadePlayoffs IS TRUE").Na().Drop();

            mvpAwardShareWithStats.Write().Format("csv").Option("header", true)
                .Save("/workspace/NBAPrediction/datasets/temp/mvpwithstats");
        }

        private void CleanupTempFiles() 
        {
            System.IO.Directory.Delete("datasets/temp", true);
        }
    }
}