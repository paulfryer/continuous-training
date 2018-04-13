using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SageMaker;
using Amazon.SageMakerRuntime;
using Amazon.SageMakerRuntime.Model;
using ContinuousTraining.EntityExtraction;
using ContinuousTraining.EntityExtraction.ContinuousTraining.EntityExtractors;
using ContinuousTraining.TextExtraction;
using DotStep.Core;
using Newtonsoft.Json;

namespace ContinuousTraining.StateMachine
{
    public sealed class Predict : StateMachine<Predict.ExtractText>
    {
        public class Context : IContext
        {
            public string Symbol {get;set;}
            public string Url { get; set; }
            public decimal PredictedValue { get; set; }
        }

        [DotStep.Core.Action(ActionName = "ssm:*")]
        [DotStep.Core.Action(ActionName = "comprehend:*")]
        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "dynamodb:*")]
        public class ExtractText : TaskState<Context, Done>
        {
            private readonly ITextExtractor textExtractor = new HtmlAgilityExtractor(); // //new DiffbotTextExtractor();
            private readonly IEntityExtractor entityExtractor = new AmazonEntityExtractor();
            private readonly IAmazonSageMakerRuntime sageMakerRuntime = new AmazonSageMakerRuntimeClient();
            private readonly IAmazonDynamoDB dynamo = new AmazonDynamoDBClient();

            public override async Task<Context> Execute(Context context)
            {
                var uri = new Uri(context.Url);
                var text = await textExtractor.ExtractText(uri);

                var entities = await entityExtractor.ExtractEntitiesAsync(text);

                var itemResult = await dynamo.GetItemAsync(new GetItemRequest
                {
                    TableName = "symbol-schema",
                    Key = new Dictionary<string, AttributeValue> { {"symbol", new AttributeValue{S = context.Symbol} } }
                });

                var columns = itemResult.Item["schema"].SS;


                var sb = new StringBuilder();
                var first = true;
                foreach (var column in columns)
                //foreach (var entity in entities)
                {
                    if (!first)
                        sb.Append(",");
                    first = false;

                    var entity = entities.FirstOrDefault(e =>
                        Extractor.ExtractEntities.MakeAttributeName(e.Provider, e.Type, e.Name) == column);

                    sb.Append(entity == null ? 0 : Convert.ToInt32(10000 * Convert.ToDecimal(entity.Score)));
                }

                var csv = sb.ToString();

                string stringResult;

                using (var stream = GenerateStreamFromString(csv))
                {
                    var result = await sageMakerRuntime.InvokeEndpointAsync(new InvokeEndpointRequest
                    {
                        EndpointName = context.Symbol,
                        ContentType = "text/csv",
                        Body = stream,
                        //Accept = "text/csv"
                    });

                    using (var reader = new StreamReader(result.Body))
                        stringResult = reader.ReadToEnd();

                }

                context.PredictedValue = Convert.ToDecimal(stringResult);


                return context;
            }

            
            public static MemoryStream GenerateStreamFromString(string s)
            {
                var stream = new MemoryStream();
                var writer = new StreamWriter(stream);
                writer.Write(s);
                writer.Flush();
                stream.Position = 0;
                return stream;
            }
        }

        public class Done : PassState
        {
            public override bool End => true;
        }
    }
}
