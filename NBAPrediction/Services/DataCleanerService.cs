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

        public DataCleanerService(IHelperService helperService) {
            _helperService = helperService;
        }

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

            // DataFrame advancedStats = _helperService.LoadFromCsv(spark, "Advanced.csv")
            //     .Select("player_id", "season", "tm", "g", "mp", "per", "ts_percent", "x3p_ar", "f_tr", "orb_percent", "drb_percent", "trb_percent",
            //     "ast_percent", "stl_percent", "blk_percent", "tov_percent", "usg_percent", "ows", "dws", "ws", "ws_48", "obpm", "dbpm", "bpm", "vorp")
            //     .Filter("tm != TOT")
            //     .Na().Replace("*", new Dictionary<string, string>() { { "NA", null } });

        }

        private void CreateTeamTables(SparkSession spark)
        {
            DataFrame teamSummaries = _helperService.LoadFromCsv(spark, "/workspace/NBAPrediction/datasets/Team Summaries.csv")
                .Filter("team != 'League Average' AND abbreviation != 'NA'");

            DataFrame teams = teamSummaries
                .Select(F.Col("team").As("TeamName"), 
                    F.Col("abbreviation").As("TeamNameShort"), 
                    F.Col("lg").As("League"))
                .Distinct()
                .WithColumn("TeamId", 
                    F.Concat(F.Hex(F.Col("League")), F.Hex(F.Col("TeamNameShort"))));

            teams.Write().Format("delta").Mode(SaveMode.Overwrite).SaveAsTable("Teams");

            DataFrame teamSeasonStats = teamSummaries
                .Join(teams, teamSummaries["abbreviation"] == teams["TeamNameShort"] 
                    & teamSummaries["lg"] == teams["League"], "inner")
                .Select(F.Col("TeamId"), 
                    F.Col("season").As("Season"),
                    F.Col("playoffs").As("MadePlayoffs").Cast("boolean"),
                    F.Col("age").As("AverageAge"),
                    F.Col("w").As("Wins").Cast("short"),
                    F.Col("l").As("Losses").Cast("short"),
                    F.Col("pw").As("PredictedWins").Cast("short"),
                    F.Col("pl").As("PredictedLosses").Cast("short"),
                    F.Col("mov").As("AverageMarginOfVictory"),
                    F.Col("sos").As("StrengthOfSchedule"),
                    F.Col("srs").As("SimpleRating"),
                    F.Col("o_rtg").As("OffensiveRating"),
                    F.Col("d_rtg").As("DefensiveRating"),
                    F.Col("n_rtg").As("NetRating"),
                    F.Col("pace").As("Pace"),
                    F.Col("f_tr").As("FreeThrowRate"),
                    F.Col("x3p_ar").As("3PointAttemptRate"),
                    F.Col("ts_percent").As("TrueShootingPercentage"),
                    F.Col("e_fg_percent").As("EffectiveFieldGoalPercentage"),
                    F.Col("tov_percent").As("TurnoverPercentage"),
                    F.Col("orb_percent").As("OffensiveReboundPercentage"),
                    F.Col("ft_fga").As("FreeThrowFactor"),
                    F.Col("opp_e_fg_percent").As("OpponentEFGPercentage"),
                    F.Col("opp_tov_percent").As("OpponentTOVPercentage"),
                    F.Col("opp_ft_fga").As("OpponentFreeThrowFactor"));

            teamSeasonStats = CastColumnsToFloat(teamSeasonStats);

            teamSeasonStats.Write().Format("delta").Mode(SaveMode.Overwrite).SaveAsTable("TeamSeasonStats");
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