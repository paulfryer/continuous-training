using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ContinuousTraining.Indexing
{
    interface IIndexingService
    {
        Task<List<Statistic>> GetStatistics(DateTime start, DateTime end, string symbol);
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

