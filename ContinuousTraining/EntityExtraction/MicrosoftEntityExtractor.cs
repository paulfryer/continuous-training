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
        public class MicrosoftEntityExtractor : IEntityExtractor
        {
            private const string KeyPhrasesApiEndpoint =
                "https://westus2.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrase";
            public string ProviderCode => "MSFT";
            private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

           

            private static string MicrosoftApimSubscriptionKey { get; set; }

            public async Task<List<ExtractedEntity>> ExtractEntitiesAsync(string text)
            {
            if (MicrosoftApimSubscriptionKey == null)
            {
                var getParameter = ssm.GetParameterAsync(new GetParameterRequest
                {
                    Name = "/CT/MicrosoftApimSubscriptionKey"
                });
                MicrosoftApimSubscriptionKey = getParameter.Result.Parameter.Value;
            }

                var entities = new List<ExtractedEntity>();

                const int maxBytesPerRequest = 5000;
                const int bytesPerCharacter = 2;
                const int maxTextSize = maxBytesPerRequest / bytesPerCharacter;
                var documents = new List<dynamic>();

                var index = 1;
                while (text.Length > 0)
                {
                    var length = text.Length > maxTextSize ? maxTextSize : text.Length;
                    var textPart = text.Substring(0, length);
                    documents.Add(new
                    {
                        language = "en",
                        id = $"{index}",
                        text = textPart
                    });
                    text = text.Remove(0, length);
                    index++;
                }

                var json = JsonConvert.SerializeObject(new
                {
                    documents
                });

                using (var httpClient = new HttpClient())
                {
                    var req = new HttpRequestMessage
                    {
                        Content = new StringContent(json),
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(KeyPhrasesApiEndpoint)
                    };

                    req.Headers.Add("Ocp-Apim-Subscription-Key", MicrosoftApimSubscriptionKey);
                    req.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                    var resp = await httpClient.SendAsync(req);

                    var response = JsonConvert.DeserializeObject<dynamic>(resp.Content.ReadAsStringAsync().Result);

                    if (response.documents == null) return entities;

                    foreach (var doc in response.documents)
                    foreach (var phrase in doc.keyPhrases)
                        entities.Add(new ExtractedEntity((string) phrase, "PHRASE",
                            Convert.ToInt32(10000 * Convert.ToDecimal(1)), ProviderCode));
                }

                return entities;
            }
        }
    }
}