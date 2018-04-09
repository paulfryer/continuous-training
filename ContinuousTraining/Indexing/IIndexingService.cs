using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ContinuousTraining.Indexing
{
    public interface IIndexingService
    {
        Task<List<Statistic>> GetStatisticsAsync(DateTime start, DateTime end, string symbol);
    }

    public class Statistic
    {
        public DateTime Date { get; set; }
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
        public decimal Volume { get; set; }
    }

    public static class Extensions
    {
        public static string ToCSV(this List<Statistic> statistics)
        {
            var sb = new StringBuilder();
            sb.AppendLine("date,open,high,low,close,volume");
            foreach (var stat in statistics)
                sb.AppendLine($"{stat.Date.ToString("yyyy-MM-dd")},{stat.Open},{stat.High},{stat.Low},{stat.Close},{stat.Volume}");

            return sb.ToString();
        }
    }
}

