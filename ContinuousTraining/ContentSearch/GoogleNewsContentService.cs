using System;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ContinuousTraining.ContentSearch
{
    public class GoogleNewsContentService : IContentService
    {
        public async Task<SearchResult> GetLinksAsync(string searchTerm, DateTime date, int startIndex)
        {
            var response = new SearchResult();

            var startPattern = date.ToString("MM/dd/yyyy");
            var searchUrl =
                $"https://www.google.com/search?q={searchTerm}&tbs=cdr:1,cd_min:{startPattern},cd_max:{startPattern}&tbm=nws&start={startIndex}";

            using (var http = new HttpClient())
            {
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                    RequestUri = new Uri(searchUrl)
                };
                request.Headers.UserAgent.ParseAdd(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36");
                var result = await http.SendAsync(request);
                Console.WriteLine($"StatusCode from news search: {result.StatusCode}");
                var html = await result.Content.ReadAsStringAsync();

                response.HasMoreResults = html.Contains("start=" + (startIndex + 10));
                var r = new Regex(@"<(a|link).*?href=(""|')(.+?)(""|').*?>");
                var mc1 = r.Matches(html);
                foreach (Match match in mc1)
                {
                    var link = match.Value;
                    if (!link.Contains("href=\"http") ||
                        !link.Contains("onmousedown=\"return rwt") && !link.Contains("ping=")) continue;

                    const string startSearchPattern = "href=\"";
                    var start = link.IndexOf(startSearchPattern, StringComparison.Ordinal);
                    link = link.Substring(start + startSearchPattern.Length,
                        link.Length - start - startSearchPattern.Length);
                    link = link.Substring(0, link.IndexOf("\" ", StringComparison.Ordinal));
                    if (response.Links.All(l => l.ToString() != link))
                        response.Links.Add(new Uri(link));
                }
            }

            return response;
        }
    }
}