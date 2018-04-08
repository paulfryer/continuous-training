using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Amazon.StepFunctions;
using Amazon.StepFunctions.Model;
using ContinuousTraining.Indexing;
using DotStep.Common.Functions;
using DotStep.Core;
using Newtonsoft.Json;
using Required = DotStep.Core.Required;

namespace ContinuousTraining.StateMachine
{
    public sealed class Crawler : StateMachine<Crawler.ApplyDefaults>
    {
        public class Context : IContext
        {
            [Required]
            public string Symbol { get; set; }
            [Required]
            public string SearchTerm { get; set; }
            [Required]
            public int MaxDateSamples { get; set; }
            [Required]
            public string BucketName { get; set; }

            public string IndexerStepFunctionArn { get; set; }
            public int BatchSize { get; set; }
            public DateTime MinDate { get; set; }
            public DateTime MaxDate { get; set; }

            public List<DateTime> Dates { get; set; }

            public bool HasMoreDatesToIndex { get; set; }
        }

        [DotStep.Core.Action(ActionName = "ssm:GetParameter")]
        public sealed class ApplyDefaults : TaskState<Context, Validate>
        {
            private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();
            public override async Task<Context> Execute(Context context)
            {
                context.Dates = new List<DateTime>();

                if (context.BatchSize <= 0)
                    context.BatchSize = 2;
                if (context.MinDate == DateTime.MinValue)
                    context.MinDate = DateTime.UtcNow.Subtract(TimeSpan.FromDays(365 * 5));
                if (context.MaxDate == DateTime.MinValue)
                    context.MaxDate = DateTime.UtcNow.Subtract(TimeSpan.FromDays(2));
                if (context.MaxDateSamples <= 0)
                    context.MaxDateSamples = 10;

                var totalDays = Convert.ToInt32(context.MaxDate.Subtract(context.MinDate).TotalDays);
                
                for (var i = 0; i < context.MaxDateSamples; i++)
                {
                    var random = new Random();
                    var days = random.Next(totalDays);
                    var date = context.MinDate.AddDays(days);
                    if (!context.Dates.Contains(date)) context.Dates.Add(date);
                }

                context.HasMoreDatesToIndex = context.Dates.Any();

                if (string.IsNullOrEmpty(context.BucketName))
                {
                    var result = await ssm.GetParameterAsync(new GetParameterRequest { Name = "/CT/BucketName" });
                    context.BucketName = result.Parameter.Value;
                }

                return context;
            }
        }

        public class Validate : ReferencedTaskState<Context, GetLatestIndexingData, ValidateMessage<Context>>
        {
        }

        [DotStep.Core.Action(ActionName = "s3:PutObject")]
        [DotStep.Core.Action(ActionName = "ssm:GetParameter")]
        public sealed class GetLatestIndexingData : TaskState<Context, SubmitIndexingJob>
        {
            private readonly IIndexingService indexingService = new AlphaVantageIndexingService();
            private IAmazonS3 s3 = new AmazonS3Client();
            public override async Task<Context> Execute(Context context)
            {
                var start = DateTime.Parse("01/01/2000");
                var end = DateTime.UtcNow;
                var stats = await indexingService.GetStatisticsAsync(start, end, context.Symbol);
                var csv = stats.ToCSV();

                var response = await s3.PutObjectAsync(new PutObjectRequest
                {
                    Key = $"ingest/index/symbol={context.Symbol}/stats.csv",
                    BucketName = context.BucketName,
                    ContentBody = csv,
                    ContentType = "text/csv"
                });

                return context;
            }
        }


        public sealed class DetermineNextStep : ChoiceState<Done>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<SubmitIndexingJob, Context>(c => c.HasMoreDatesToIndex == true)
            };
        }


        public sealed class WaitBetweenBatches : WaitState<DetermineNextStep>
        {
            public override int Seconds => 60;
        }

        [DotStep.Core.Action(ActionName = "states:*")]
        public sealed class SubmitIndexingJob : TaskState<Context, WaitBetweenBatches>
        {
            private readonly IAmazonStepFunctions stepFunctions = new AmazonStepFunctionsClient();

            public override async Task<Context> Execute(Context context)
            {
                if (string.IsNullOrEmpty(context.IndexerStepFunctionArn))
                {
                    var response = await stepFunctions.ListStateMachinesAsync(new ListStateMachinesRequest());
                    context.IndexerStepFunctionArn = response.StateMachines
                        .Single(sm => sm.Name.StartsWith(typeof(Extractor).Name)).StateMachineArn;
                }

                var tasks = new List<Task>();
                for (var i = 0; i < context.BatchSize; i++)
                    if (context.Dates.Any())
                    {
                        var date = context.Dates.First();
                        context.Dates.RemoveAt(0);

                        var input = new
                        {
                            context.SearchTerm,
                            SearchDate = date
                        };
                        var json = JsonConvert.SerializeObject(input);
                        Console.Write("Sending job: " + json);
                        var task = stepFunctions.StartExecutionAsync(new StartExecutionRequest
                        {
                            Name =
                                $"{context.SearchTerm}-{date.Year}-{date.Month:D2}-{date.Day:D2}",
                            StateMachineArn = context.IndexerStepFunctionArn,
                            Input = json
                        });
                        tasks.Add(task);
                    }

                await Task.WhenAll(tasks);
                context.HasMoreDatesToIndex = context.Dates.Any();
                return context;
            }
        }

        public sealed class Done : PassState
        {
            public override bool End => true;
        }
    }
}