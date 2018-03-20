using System;
using System.Net.Http;
using System.Threading.Tasks;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Newtonsoft.Json;

namespace ContinuousTraining.TextExtraction
{
    public class DiffbotTextExtractor : ITextExtractor
    {
        private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

    

        private static string DiffbotToken { get; set; }

        public async Task<string> ExtractText(Uri url)
        {
            if (DiffbotToken == null)
            {
                var getParameter = ssm.GetParameterAsync(new GetParameterRequest
                {
                    Name = "/CT/DiffbotToken",
                    WithDecryption = true
                });
                DiffbotToken = getParameter.Result.Parameter.Value;
            }

            using (var httpClient = new HttpClient())
            {
                var encodedUrl = Uri.EscapeDataString(url.ToString());
                var req = new HttpRequestMessage
                {
                    RequestUri =
                        new Uri($"https://api.diffbot.com/v3/article?token={DiffbotToken}&url={encodedUrl}")
                };
                var resp = await httpClient.SendAsync(req);
                var json = await resp.Content.ReadAsStringAsync();
                var obj = JsonConvert.DeserializeObject<dynamic>(json);
                if (obj != null && obj.objects != null)
                    return obj.objects[0].text;
                return null;
            }
        }
    }
}