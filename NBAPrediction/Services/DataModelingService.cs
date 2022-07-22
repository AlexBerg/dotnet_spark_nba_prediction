using System.Collections.Generic;
using Microsoft.Spark.Sql;
using System.Linq;

using F = Microsoft.Spark.Sql.Functions;
using T = Microsoft.Spark.Sql.Types;

namespace NBAPrediction.Services
{
    internal class DataModelingService : IDataModelingService
    {
        private readonly IHelperService _helperService;
        private readonly string _pathToRaw = "/workspace/NBAPrediction/datasets/";

        public DataModelingService(IHelperService helperService) {
            _helperService = helperService;
        }

        public void CreateNBADeltaTables(SparkSession spark)
        {
            var teams = CreateTeamTables(spark);

            CreatePlayerTables(spark, teams);
        }

        private DataFrame CreateTeamTables(SparkSession spark)
        {
            DataFrame teamSummaries = _helperService.LoadFromCsv(spark, _pathToRaw + "Team Summaries.csv")
                .Filter("team != 'League Average' AND abbreviation != 'NA'");

            DataFrame teams = teamSummaries
                .Select(F.Col("team").As("TeamName"), 
                    F.Col("abbreviation").As("TeamNameShort"), 
                    F.Col("lg").As("League"))
                .Distinct()
                .WithColumn("TeamId", 
                    F.Concat(F.Hex(F.Col("League")), F.Hex(F.Col("TeamNameShort"))));

            _helperService.CreateOrOverwriteManagedDeltaTable(teams, "Teams");

            DataFrame teamSeasonStats = teamSummaries
                .Join(teams, teamSummaries["abbreviation"] == teams["TeamNameShort"] 
                    & teamSummaries["lg"] == teams["League"], "inner")
                .Select(F.Col("TeamId"), 
                    F.Col("season").As("Season"),
                    F.Col("playoffs").As("MadePlayoffs").Cast("boolean"),
                    F.Col("age").As("AverageAge"),
                    F.Col("w").As("Wins").Cast("short"),
                    F.Col("l").As("Losses").Cast("short"),
                    (F.Col("w").Cast("short") + F.Col("l").Cast("short")).As("GamesPlayed"),
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
                    F.Col("x3p_ar").As("ThreePointAttemptRate"),
                    F.Col("ts_percent").As("TrueShootingPercentage"),
                    F.Col("e_fg_percent").As("EffectiveFieldGoalPercentage"),
                    F.Col("tov_percent").As("TurnoverPercentage"),
                    F.Col("orb_percent").As("OffensiveReboundPercentage"),
                    F.Col("ft_fga").As("FreeThrowFactor"),
                    F.Col("opp_e_fg_percent").As("OpponentEFGPercentage"),
                    F.Col("opp_tov_percent").As("OpponentTOVPercentage"),
                    F.Col("opp_ft_fga").As("OpponentFreeThrowFactor"));

            teamSeasonStats = CastColumnsToFloat(teamSeasonStats);

            _helperService.CreateOrOverwriteManagedDeltaTable(teamSeasonStats, "TeamSeasonStats");

            return teams;
        }

        private void CreatePlayerTables(SparkSession spark, DataFrame teams) 
        {
            CreatePlayersTable(spark);

            CreatePlayerAwardShareTable(spark, teams);

            CreatePlayerSeasonAdvancedStatsTable(spark, teams);

            CreatePlayerSeasonStatsTable(spark, teams);
        }

        private void CreatePlayersTable(SparkSession spark) 
        {
            var playerInfo = _helperService.LoadFromCsv(spark, _pathToRaw + "Player Career Info.csv")
                .Select(F.Col("player_id").As("PlayerId"),
                    F.Col("player").As("PlayerName"),
                    F.Col("hof").As("MadeHallOfFame").Cast("boolean"));
            
            playerInfo.Write().Format("delta").Mode(SaveMode.Overwrite).SaveAsTable("Players");

            _helperService.CreateOrOverwriteManagedDeltaTable(playerInfo, "Players");
        }

        private void CreatePlayerAwardShareTable(SparkSession spark, DataFrame teams) 
        {
            var playerAwardShare = _helperService.LoadFromCsv(spark, _pathToRaw + "Player Award Shares.csv");
            playerAwardShare = playerAwardShare.Join(teams, playerAwardShare["tm"] == teams["TeamNameShort"])
                .Select(F.Col("award").As("Award"),
                    F.Col("player_id").As("PlayerId"),
                    F.Col("season").As("Season"),
                    F.Col("TeamId"),
                    F.Col("share").As("PointsWon").Cast("short"),
                    F.Col("pts_max").As("MaxPointsPossible").Cast("short"),
                    F.Col("share").As("Share").Cast("float"),
                    F.Col("winner").As("WonAward").Cast("boolean"));

            _helperService.CreateOrOverwriteManagedDeltaTable(playerAwardShare, "PlayerSeasonAwardShare");
        }

        private void CreatePlayerSeasonAdvancedStatsTable(SparkSession spark, DataFrame teams) 
        {
            var advancedStats = _helperService.LoadFromCsv(spark, _pathToRaw + "Advanced.csv").Filter("tm != 'TOT'");
            advancedStats = advancedStats.Join(teams, advancedStats["tm"] == teams["TeamNameShort"])
                .Select(F.Col("player_id").As("PlayerId"),
                    F.Col("season").As("Season"),
                    F.Col("TeamId"),
                    F.Col("per").As("PlayerEfficiencyRating"),
                    F.Col("ts_percent").As("TrueShootingPercentage"),
                    F.Col("x3p_ar").As("ThreePointAttemptRate"),
                    F.Col("f_tr").As("FreeThrowRate"),
                    F.Col("orb_percent").As("OffensiveReboundPercentage"),
                    F.Col("drb_percent").As("DefensiveReboundPercentage"),
                    F.Col("trb_percent").As("TotalReboundPercentage"),
                    F.Col("ast_percent").As("AssistPercentage"),
                    F.Col("stl_percent").As("StealPercentage"),
                    F.Col("blk_percent").As("BlockPercentage"),
                    F.Col("tov_percent").As("TurnoverPercentage"),
                    F.Col("usg_percent").As("UsagePercentage"),
                    F.Col("ows").As("OffensiveWinShares"),
                    F.Col("dws").As("DefensiveWinShares"),
                    F.Col("ws").As("WinShares"),
                    F.Col("ws_48").As("WinSharesPer48"),
                    F.Col("obpm").As("OffensiveBoxPlusMinus"),
                    F.Col("dbpm").As("DefensiveBoxPlusMinus"),
                    F.Col("bpm").As("BoxPlusMinus"),
                    F.Col("vorp").As("ValueOverReplacementPlayer"))
                .Na().Replace("*", new Dictionary<string, string>() { { "NA", null } });

            advancedStats = CastColumnsToFloat(advancedStats);

            _helperService.CreateOrOverwriteManagedDeltaTable(advancedStats, "PlayerSeasonAdvancedStats");
        }

        private void CreatePlayerSeasonStatsTable(SparkSession spark, DataFrame teams) 
        {
            var playerPerGameStats = _helperService.LoadFromCsv(spark, _pathToRaw + "Player Per Game.csv").Filter("tm != 'TOT'");
            playerPerGameStats = playerPerGameStats.Join(teams, playerPerGameStats["tm"] == teams["TeamNameShort"])
                .Select(F.Col("player_id").As("PlayerId"),
                    F.Col("season").As("Season"),
                    F.Col("TeamId"),
                    F.Col("g").As("GamesPlayed").Cast("short"),
                    F.Col("gs").As("GamesStarted").Cast("short"),
                    F.Col("mp_per_game").As("MinutesPerGame"),
                    F.Col("pts_per_game").As("PointsPerGame"),
                    F.Col("fg_per_game").As("FieldGoalsPerGame"),
                    F.Col("fga_per_game").As("FieldGoalsAttemptedPerGame"),
                    F.Col("fg_percent").As("FieldGoalPercentage"),
                    F.Col("x3p_per_game").As("ThreePointersPerGame"),
                    F.Col("x3pa_per_game").As("ThreePointersAttemptedPerGame"),
                    F.Col("x3p_percent").As("ThreePointerPercentage"),
                    F.Col("x2p_per_game").As("TwoPointersPerGame"),
                    F.Col("x2pa_per_game").As("TwoPointersAttemptedPerGame"),
                    F.Col("x2p_percent").As("TwoPointerPercentage"),
                    F.Col("e_fg_percent").As("EffectiveFieldGoalPercentage"),
                    F.Col("ft_per_game").As("FreeThrowsPerGame"),
                    F.Col("fta_per_game").As("FreeThrowsAttemptedPerGame"),
                    F.Col("ft_percent").As("FreeThrowPercentage"),
                    F.Col("orb_per_game").As("OffensiveReboundsPerGame"),
                    F.Col("drb_per_game").As("DefensiveReboundsPerGame"),
                    F.Col("trb_per_game").As("TotalReboundsPerGame"),
                    F.Col("ast_per_game").As("AssistsPerGame"),
                    F.Col("stl_per_game").As("StealsPerGame"),
                    F.Col("blk_per_game").As("BlocksPerGame"),
                    F.Col("tov_per_game").As("TurnoversPerGame"),
                    F.Col("pf_per_game").As("PersonalFoulsPerGame"))
                .Na().Replace("*", new Dictionary<string, string>() { { "NA", null } });

            playerPerGameStats = CastColumnsToFloat(playerPerGameStats);

            _helperService.CreateOrOverwriteManagedDeltaTable(playerPerGameStats, "PlayerSeasonStats");
        }

        private DataFrame CastColumnsToFloat(DataFrame dataFrame) 
        {
            var cols = dataFrame.Schema().Fields
                .Where(f => f.DataType.GetType() == typeof(T.StringType))
                .Select(f => f.Name);
            foreach(string col in cols)
            {
                if(col != "Season" && col != "PlayerId" && col != "TeamId" && col != "Award")
                    dataFrame = dataFrame.WithColumn(col, F.Col(col).Cast("float"));
            }

            return dataFrame;
        }
    }
}