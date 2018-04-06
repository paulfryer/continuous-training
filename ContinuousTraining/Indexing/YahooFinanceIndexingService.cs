using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace ContinuousTraining.Indexing
{
    public class YahooFinanceIndexingService : IIndexingService
    {
        public async Task<List<Statistic>> GetStatistics(DateTime start, DateTime end, string symbol)
        {

            var period1 = ToEpoch(start);
            var period2 = ToEpoch(end);


            using (var httpClient = new HttpClient())
            {
                var url =
                    $"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={period1}&period2={period2}&interval=1d&events=history&crumb=4TOIxD3iqxw";

                var response = await httpClient.GetAsync(url);


                var csv = await response.Content.ReadAsStringAsync();
            }


            throw new NotImplementedException();


        }


        private int ToEpoch(DateTime date)
        {
            TimeSpan t = date - new DateTime(1970, 1, 1);
            int secondsSinceEpoch = (int)t.TotalSeconds;
            return secondsSinceEpoch;
        }
    }
}