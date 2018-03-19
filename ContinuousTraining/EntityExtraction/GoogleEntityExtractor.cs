using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Newtonsoft.Json;

namespace ContinuousTraining.EntityExtraction
{
    namespace ContinuousTraining.EntityExtractors
    {
        public class GoogleEntityExtractor : IEntityExtractor
        {
            private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

            public GoogleEntityExtractor()
            {
                var getParameter = ssm.GetParameterAsync(new GetParameterRequest
                {
                    Name = "GoogleApiKey",
                    WithDecryption = true
                });
                GoogleApiKey = getParameter.Result.Parameter.Value;
            }

            private string GoogleApiKey { get; }

            private string EntitySentimentApiEndpoint =>
                $"https://language.googleapis.com/v1beta2/documents:analyzeEntitySentiment?key={GoogleApiKey}";


            public async Task<List<ExtractedEntity>> ExtractEntitiesAsync(string text)
            {
                var entities = new List<ExtractedEntity>();

                using (var httpClient = new HttpClient())
                {
                    var reqObj = new
                    {
                        document = new
                        {
                            type = "PLAIN_TEXT",
                            content = text
                        }
                    };
                    var reqJson = JsonConvert.SerializeObject(reqObj);

                    var req = new HttpRequestMessage
                    {
                        Content = new StringContent(reqJson),
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(EntitySentimentApiEndpoint)
                    };
                    req.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                    var resp = await httpClient.SendAsync(req);
                    var response = JsonConvert.DeserializeObject<dynamic>(await resp.Content.ReadAsStringAsync());

                    if (response.entities == null) return entities;

                    foreach (var e in response.entities)
                        if (decimal.TryParse((string) e.salience, out _))
                            entities.Add(
                                new ExtractedEntity((string) e.name,
                                    (string) e.type, Convert.ToInt32(10000 * Convert.ToDecimal((string) e.salience))));
                }

                return entities;
            }
        }
    }
}