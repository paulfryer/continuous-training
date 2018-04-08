using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using ContinuousTraining.ContentSearch;
using ContinuousTraining.EntityExtraction;
using ContinuousTraining.EntityExtraction.ContinuousTraining.EntityExtractors;
using ContinuousTraining.TextExtraction;
using DotStep.Core;
using Newtonsoft.Json;

namespace ContinuousTraining.StateMachine
{
    public sealed class Extractor : StateMachine<Extractor.GetNews>
    {
        private const int DefaultMaxResultsPerSearch = 50;

        public class Context : IContext
        {
            public string SearchTerm { get; set; }

            public string Url { get; set; }
            public string Text { get; set; }

            public string UrlMd5 { get; set; }

            public dynamic Entities { get; set; }

            public DateTime IndexTime { get; set; }

            public DateTime SearchDate { get; set; }
            public bool HasMoreResults { get; set; }
            public int StartIndex { get; set; }

            public List<string> Results { get; set; }
            public bool MoreResultsToProcess { get; set; }

            public int MaxResultsPerSearch { get; set; }
            
        }

        public sealed class PageIterator : ChoiceState<CheckForMoreResultsToProcess>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<WaitBetweenPageGrabs, Context>(c => c.HasMoreResults == true)
            };
        }

        public sealed class WaitBetweenPageGrabs : WaitState<GetNews>
        {
            public override int Seconds => 3;
        }

        public sealed class GetNews : TaskState<Context, PageIterator>
        {
            private readonly IContentService contentService = new GoogleNewsContentService();

            public override async Task<Context> Execute(Context context)
            {
                if (context.MaxResultsPerSearch <= 0)
                    context.MaxResultsPerSearch = DefaultMaxResultsPerSearch;

                if (context.Results == null)
                    context.Results = new List<string>();

                try
                {
                    var result =
                        await contentService.GetLinksAsync(context.SearchTerm, context.SearchDate, context.StartIndex);

                    context.Results = result.Links.Select(l => l.ToString()).ToList();
                }
                catch (Exception)
                {
                    context.MoreResultsToProcess = false;
                }
                finally
                {
                    if (context.HasMoreResults)
                    {
                        context.StartIndex += 10;
                        context.HasMoreResults = context.StartIndex < context.MaxResultsPerSearch;
                    }

                    context.MoreResultsToProcess = context.Results.Count > 0;
                }

                return context;
            }
        }

        public sealed class CheckForMoreResultsToProcess : ChoiceState<Done>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<ExtractContent, Context>(c => c.MoreResultsToProcess == true)
            };
        }

        [DotStep.Core.Action(ActionName = "ssm:GetParameter")]
        public sealed class ExtractContent : TaskState<Context, ExtractEntities>
        {
            private readonly ITextExtractor textExtractor = new DiffbotTextExtractor();

            public override async Task<Context> Execute(Context @event)
            {
                @event.Text = null;
                @event.Url = @event.Results.First();
                @event.Results.RemoveAt(0);
                @event.MoreResultsToProcess = @event.Results.Count > 0;
                try
                {
                    @event.Text = await textExtractor.ExtractText(new Uri(@event.Url));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ERROR while trying to extract content. {ex.Message}");
                }

                return @event;
            }
        }

        [DotStep.Core.Action(ActionName = "comprehend:*")]
        [DotStep.Core.Action(ActionName = "firehose:*")]
        [DotStep.Core.Action(ActionName = "dynamodb:*")]
        public sealed class ExtractEntities : TaskState<Context, CheckForMoreResultsToProcess>
        {
            private readonly List<IEntityExtractor> entityExtractors = new List<IEntityExtractor>
            {
                new AmazonEntityExtractor()
            };

            private readonly IAmazonKinesisFirehose firehose = new AmazonKinesisFirehoseClient();

            public override async Task<Context> Execute(Context context)
            {
                if (string.IsNullOrEmpty(context.Text))
                {
                    Console.WriteLine("No text found so skipping exraction..");
                    return context;
                }

                var extractionTasks = new List<Task<List<ExtractedEntity>>>();

                foreach (var entityExtractor in entityExtractors)
                    extractionTasks.Add(entityExtractor.ExtractEntitiesAsync(context.Text));

                await Task.WhenAll(extractionTasks);

                var item = new Dictionary<string, int>();
                var records = new List<Record>();
                context.UrlMd5 = CalculateMd5Hash(context.Url);
                foreach (var extractionTask in extractionTasks)
                foreach (var entity in extractionTask.Result)
                {
                    AddRecord(records, context.SearchTerm, context.IndexTime, entity.Provider, context.UrlMd5,
                        entity.Type, entity.Name, entity.Score);
                    if (!decimal.TryParse(Convert.ToString(entity.Score, CultureInfo.InvariantCulture),
                        out var decimalValue)) continue;
                    var name = MakeAttributeName(entity.Provider, entity.Type, entity.Name);
                    item[name] = Convert.ToInt32(10000 * Convert.ToDecimal(decimalValue));
                }


                var firehoseTasks = new List<Task>();
                const int maxRecordsPerBatch = 100;
                while (records.Count > 0)
                {
                    var totalRecords = records.Count;
                    var recordsToTake = totalRecords > maxRecordsPerBatch ? maxRecordsPerBatch : totalRecords;
                    var recordSet = records.Take(recordsToTake).ToList();
                    var firehoseTask = firehose.PutRecordBatchAsync(new PutRecordBatchRequest
                    {
                        DeliveryStreamName = "ingest-entity",
                        Records = recordSet
                    });
                    firehoseTasks.Add(firehoseTask);
                    records.RemoveRange(0, recordsToTake);
                }

                var itemJson = JsonConvert.SerializeObject(item);
                dynamic itemDynamic = JsonConvert.DeserializeObject(itemJson);
                itemDynamic.UrlMD5 = context.UrlMd5;
                itemDynamic.Url = context.Url;
                itemDynamic.Date = context.SearchDate.Date;
                itemJson = JsonConvert.SerializeObject(itemDynamic) + "\n";
                var itemFirehoseTask = firehose.PutRecordAsync(new PutRecordRequest
                {
                    DeliveryStreamName = "ingest-item",
                    Record = new Record
                    {
                        Data = GenerateStreamFromString(itemJson)
                    }
                });
                firehoseTasks.Add(itemFirehoseTask);
                await Task.WhenAll(extractionTasks);
                context.Entities = null;
                return context;
            }

            private static void AddRecord(ICollection<Record> records,
                string searchTerm,
                DateTime indexTime,
                string provider,
                string urlMd5,
                dynamic type,
                dynamic name,
                dynamic score)
            {
                var attribute = MakeAttributeName(provider, (string) type, (string) name);
                var r = new
                {
                    ST = searchTerm,
                    AT = attribute,
                    IT = indexTime,
                    PR = provider,
                    UM = urlMd5,
                    TY = type,
                    NM = name,
                    SC = score
                };
                var st = $"{JsonConvert.SerializeObject(r)}\n";
                using (var s = GenerateStreamFromString(st))
                {
                    records.Add(new Record
                    {
                        Data = s
                    });
                }
            }

            public static string MakeAttributeName(string provider, string type, string name)
            {
                const int maxLength = 255;
                var rgx = new Regex("[^a-zA-Z0-9 -]");
                type = rgx.Replace(type, "");
                type = type.Replace(" ", "-");
                name = rgx.Replace(name, "");
                name = name.Replace(" ", "-");
                var attribute = string.Format("{0}.{1}.{2}", provider, type, name);
                attribute = attribute.ToLower();
                if (attribute.Length > maxLength)
                    attribute = attribute.Substring(0, maxLength);
                return attribute;
            }

            private static MemoryStream GenerateStreamFromString(string s)
            {
                var stream = new MemoryStream();
                var writer = new StreamWriter(stream);
                writer.Write(s);
                writer.Flush();
                stream.Position = 0;
                return stream;
            }

            private static string CalculateMd5Hash(string input)
            {
                var md5 = MD5.Create();
                var inputBytes = Encoding.ASCII.GetBytes(input);
                var hash = md5.ComputeHash(inputBytes);
                var sb = new StringBuilder();
                foreach (var t in hash)
                    sb.Append(t.ToString("X2"));
                return sb.ToString();
            }
        }

        public sealed class Done : PassState
        {
            public override bool End => true;
        }
    }
}