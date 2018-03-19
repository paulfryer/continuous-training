using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Amazon.Comprehend;
using Amazon.Comprehend.Model;

namespace ContinuousTraining.EntityExtraction
{
    namespace ContinuousTraining.EntityExtractors
    {
        public class AmazonEntityExtractor : IEntityExtractor
        {
            private static readonly IAmazonComprehend Comprehend = new AmazonComprehendClient();

            public string ProviderCode => "AMZN";

            public async Task<List<ExtractedEntity>> ExtractEntitiesAsync(string text)
            {
                var entities = new List<ExtractedEntity>();

                const int maxBytesPerRequest = 5000;
                const int bytesPerCharacter = 2;
                const int maxTextSize = maxBytesPerRequest / bytesPerCharacter;

                var textList = new List<string>();
                while (text.Length > 0)
                {
                    var length = text.Length > maxTextSize ? maxTextSize : text.Length;
                    var textPart = text.Substring(0, length);
                    textList.Add(textPart);
                    text = text.Remove(0, length);
                }

                var detectionResult = await Comprehend.BatchDetectEntitiesAsync(new BatchDetectEntitiesRequest
                {
                    LanguageCode = LanguageCode.En,
                    TextList = textList
                });

                foreach (var t in detectionResult.ResultList)
                foreach (var e in t.Entities)
                {
                    if (!decimal.TryParse(Convert.ToString(e.Score, CultureInfo.InvariantCulture),
                        out var decimalValue)) continue;

                    var entity = new ExtractedEntity
                    {
                        Name = e.Text,
                        Type = e.Type.Value,
                        Score = Convert.ToInt32(10000 * Convert.ToDecimal(decimalValue))
                    };

                    entities.Add(entity);
                }

                return entities;
            }
        }
    }
}