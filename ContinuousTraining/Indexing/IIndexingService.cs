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
}

