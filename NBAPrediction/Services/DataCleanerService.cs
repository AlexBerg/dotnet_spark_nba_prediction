using System.Collections.Generic;
using Microsoft.Spark.Sql;
using System.Linq;

using F = Microsoft.Spark.Sql.Functions;
using T = Microsoft.Spark.Sql.Types;

namespace NBAPrediction.Services
{
    internal class DataCleanerService : IDataCleanerService
    {
        private readonly IHelperService _helperService;
        

        public void CreateNBADeltaTables(SparkSession spark)
        {
            List<string> csvFileNames = new List<string> {
                "Advanced",
                "End Of Season Teams",
                "Player Per Game",
                "Player Award Shares",
                "Player Career Info",
                "Team Summaries"
            };

            DataFrame advancedStats = _helperService.LoadFromCsv(spark, "Advanced.csv")
                .Select("player_id", "season", "tm", "g", "mp", "per", "ts_percent", "x3p_ar", "f_tr", "orb_percent", "drb_percent", "trb_percent",
                "ast_percent", "stl_percent", "blk_percent", "tov_percent", "usg_percent", "ows", "dws", "ws", "ws_48", "obpm", "dbpm", "bpm", "vorp")
                .Filter("tm != TOT")
                .Na().Replace("*", new Dictionary<string, string>() { { "NA", null } });

        }

        private void CreateTeamTables(SparkSession spark)
        {
            DataFrame teamSummaries = _helperService.LoadFromCsv(spark, "Team Summaries.csv")
                .Filter("team != 'League Average'");

            DataFrame teams = teamSummaries
                .Select(F.Col("team").As("TeamName"), 
                    F.Col("abbreviation").As("TeamNameShort"), 
                    F.Col("lg").As("League"))
                .Distinct()
                .WithColumn("TeamId", F.MonotonicallyIncreasingId().Cast("string"));

            teams.Write().Format("delta").Mode(SaveMode.Overwrite).SaveAsTable("Teams");

            DataFrame teamSeasonStats = teamSummaries
                .Select(F.Col("season").As("Season"),
                    F.Col("abbreviation"),
                    F.Col("playoffs").As("MadePlayoffs").Cast("boolean"),
                    F.Col("age").As("AverageAge"),
                    F.Col("w").As("Wins").Cast("short"),
                    F.Col("l").As("Losses").Cast("short"),
                    F.Col("pw").As("PredictedWins").Cast("short"),
                    F.Col("pl").As("PredictedLosses").Cast("short"),
                    F.Col("mov").As("AverageMarginOfVictory"),
                    F.Col("sos").As("StrengthOfSchedule"),)
        }

        private DataFrame CastColumnsToFloat(DataFrame dataFrame) 
        {
            var cols = dataFrame.Schema().Fields.Where(f => f.DataType.GetType().Name == "string").Select(f => f.Name);
            foreach(string col in cols)
            {
                if(col != "Season" && col != "PlayerId" && col != "TeamId")
                dataFrame = dataFrame.WithColumn(col, F.Col(col).Cast("float"));
            }

            return dataFrame;
        }
    }
}