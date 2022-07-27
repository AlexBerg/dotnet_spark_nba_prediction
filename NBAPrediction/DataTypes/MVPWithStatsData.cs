using Microsoft.ML.Data;

namespace NBAPrediction.DataTypes
{
    public class MVPWithStatsData
    {
        [LoadColumn(0)]
        public float PlayerId;
        [LoadColumn(1)]
        public float Season;
        [LoadColumn(2)]
        public string TeamId;
        [LoadColumn(3)]
        public float PlayerEfficiencyRating;
        [LoadColumn(4)]
        public float TrueShootingPercentage;
        [LoadColumn(5)]
        public float ThreePointAttemptRate;
        [LoadColumn(6)]
        public float FreeThrowRate;
        [LoadColumn(7)]
        public float OffensiveReboundPercentage;
        [LoadColumn(8)]
        public float DefensiveReboundPercentage;
        [LoadColumn(9)]
        public float TotalReboundPercentage;
        [LoadColumn(10)]
        public float AssistPercentage;
        [LoadColumn(11)]
        public float StealPercentage;
        [LoadColumn(12)]
        public float BlockPercentage;
        [LoadColumn(13)]
        public float TurnoverPercentage;
        [LoadColumn(14)]
        public float UsagePercentage;
        [LoadColumn(15)]
        public float OffensiveWinShares;
        [LoadColumn(16)]
        public float DefensiveWinShares;
        [LoadColumn(17)]
        public float WinShares;
        [LoadColumn(18)]
        public float WinSharesPer48;
        [LoadColumn(19)]
        public float OffensiveBoxPlusMinus;
        [LoadColumn(20)]
        public float DefensiveBoxPlusMinus;
        [LoadColumn(21)]
        public float BoxPlusMinus;
        [LoadColumn(22)]
        public float ValueOverReplacementPlayer;
        [LoadColumn(23)]
        public float PointsPerGame;
        [LoadColumn(24)]
        public float GamesPlayed;
        [LoadColumn(25)]
        public float GamesStarted;
        [LoadColumn(26)]
        public float MinutesPerGame;
        [LoadColumn(27)]
        public float TeamGamesPlayed;
        [LoadColumn(28)]
        public string League;
        [LoadColumn(29)]
        public float WinPercentage;
        [LoadColumn(29)]
        public float AverageMarginOfVictory;
        [LoadColumn(30)]
        public float Share;
        [LoadColumn(31)]
        public string Award;
        [LoadColumn(32)]
        public bool WonAward;
    }
}