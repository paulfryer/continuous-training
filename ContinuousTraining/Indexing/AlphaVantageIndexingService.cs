using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ContinuousTraining.Indexing
{
    public class AlphaVantageIndexingService : IIndexingService
    {
        private static string apiKey;
        private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

        public async Task<List<Statistic>> GetStatisticsAsync(DateTime start, DateTime end, string symbol)
        {
            if (string.IsNullOrEmpty(apiKey))
            {
                var response =
                    await ssm.GetParameterAsync(new GetParameterRequest
                    {
                        Name = "/CT/AlphaVantageAPIKey"
                    });
                apiKey = response.Parameter.Value;
            }
            var outputSize = end.Subtract(start).Days <= 100 ? "compact" : "full";
            using (var httpClient = new HttpClient())
            {
                var url =
                    $"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AMZN&apikey={apiKey}&outputsize={outputSize}";
                var response = await httpClient.GetAsync(url);
                var json = await response.Content.ReadAsStringAsync();
                var obj = JsonConvert.DeserializeObject<dynamic>(json);
                var stats = new List<Statistic>();
                foreach (JProperty item in obj["Time Series (Daily)"])
                {
                    var value = item.Value;
                    stats.Add(new Statistic
                    {
                        Date = DateTime.Parse(item.Name),
                        Open = Convert.ToDecimal(value.Value<string>("1. open")),
                        High = Convert.ToDecimal(value.Value<string>("2. high")),
                        Low = Convert.ToDecimal(value.Value<string>("3. low")),
                        Close = Convert.ToDecimal(value.Value<string>("4. close")),
                        Volume = Convert.ToDecimal(value.Value<string>("5. volume")),
                    });
                }
                return stats.Where(s => s.Date >= start && s.Date <= end).ToList();
            }
        }
    }
}