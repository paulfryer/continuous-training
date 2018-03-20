using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.StepFunctions;
using Amazon.StepFunctions.Model;
using DotStep.Core;
using Newtonsoft.Json;

namespace ContinuousTraining.StateMachine
{
    public sealed class Crawler : StateMachine<Crawler.SelectDates>
    {
        public class Context : IContext
        {
            public string SearchTerm { get; set; }
            public int MaxDateSamples { get; set; }

            public string IndexerStepFunctionArn { get; set; }
            public int BatchSize { get; set; }
            public DateTime MinDate { get; set; }
            public DateTime MaxDate { get; set; }

            public List<DateTime> Dates { get; set; }

            public bool HasMoreDatesToIndex { get; set; }
        }

        public sealed class DetermineNextStep : ChoiceState<Done>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<SubmitIndexingJob, Context>(c => c.HasMoreDatesToIndex)
            };
        }

        public sealed class WaitBetweenBatches : WaitState<DetermineNextStep>
        {
            public override int Seconds => 60;
        }

        [DotStep.Core.Action(ActionName = "states:*")]
        public sealed class SubmitIndexingJob : TaskState<Context, WaitBetweenBatches>
        {
            readonly IAmazonStepFunctions stepFunctions = new AmazonStepFunctionsClient();

            public override async Task<Context> Execute(Context context)
            {
                if (string.IsNullOrEmpty(context.IndexerStepFunctionArn))
                    throw new Exception("IndexerStepFunctionArn is required.");

                var tasks = new List<Task>();
                for (var i = 0; i < context.BatchSize; i++)
                {
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
                }

                await Task.WhenAll(tasks);
                context.HasMoreDatesToIndex = context.Dates.Any();
                return context;
            }
        }

        public sealed class SelectDates : TaskState<Context, SubmitIndexingJob>
        {
            public override async Task<Context> Execute(Context context)
            {
                context.Dates = new List<DateTime>();

                if (context.BatchSize <= 0)
                    context.BatchSize = 2;
                if (context.MinDate == DateTime.MinValue)
                    context.MinDate = DateTime.UtcNow.Subtract(TimeSpan.FromDays(365 * 5));
                if (context.MaxDate == DateTime.MinValue)
                    context.MaxDate = DateTime.UtcNow.Subtract(TimeSpan.FromDays(2));

                var totalDays = Convert.ToInt32(context.MaxDate.Subtract(context.MinDate).TotalDays);

                for (var i = 0; i < context.MaxDateSamples; i++)
                {
                    var random = new Random();
                    var days = random.Next(totalDays);
                    var date = context.MinDate.AddDays(days);
                    if (!context.Dates.Contains(date))
                    {
                        context.Dates.Add(date);
                    }
                }

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