using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SageMaker;
using Amazon.SageMaker.Model;
using DotStep.Core;

namespace ContinuousTraining.StateMachine
{
    public sealed class Retrain : StateMachine<Retrain.ReCrawlEntities>
    {
        public class Context : IContext
        {
            public string SearchTerm { get; set; }

            public string TableName { get; set; }

            public string QueryExecutionId { get; set; }
            public string ResultsBucketName { get; set; }

            public string TrainingBucketName { get; set; }
            public string TrainingKeyName { get; set; }
            public string ValidationKeyName { get; set; }

            public string TrainingJobName { get; set; }
            public string TrainingImage { get; set; }
            public string TrainingRoleArn { get; set; }
            public string TrainingJobArn { get; set; }
            public string TrainingJobStatus { get; set; }

            public string ModelArn { get; set; }

            public string EndpointConfigArn { get; set; }

            public bool EndpointExists { get; set; }

            public string EndpointArn { get; set; }
            public string QueryExecutionBucket { get; set; }
        }

        public sealed class Done : EndState
        {
        }

        public sealed class WaitForQueryToComplete : WaitState<StoreTrainingData>
        {
            public override int Seconds => 10;
        }

        public sealed class WaitForCrawlingToComplete : WaitState<CreateTable>
        {
            public override int Seconds => 40;
        }

        public sealed class WaitForTrainingToComplete : WaitState<CheckTrainingStatus>
        {
            public override int Seconds => 30;
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class CheckTrainingStatus : TaskState<Context, DetermineNextTrainingStep>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var status = await sageMaker.DescribeTrainingJobAsync(new DescribeTrainingJobRequest
                {
                    TrainingJobName = context.TrainingJobName
                });
                context.TrainingJobStatus = status.TrainingJobStatus;

                return context;
            }
        }

        public sealed class DetermineNextTrainingStep : ChoiceState
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<WaitForTrainingToComplete, Context>(c => c.TrainingJobStatus == "InProgress"),
                new Choice<RegisterModel, Context>(c => c.TrainingJobStatus == "Completed")
            };
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        private sealed class RegisterModel : TaskState<Context, CreateEndpointConfiguration>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateModelAsync(new CreateModelRequest
                {
                    ExecutionRoleArn = context.TrainingRoleArn,
                    ModelName = context.TrainingJobName,
                    PrimaryContainer = new ContainerDefinition
                    {
                        Image = context.TrainingImage,
                        ModelDataUrl =
                            $"https://s3-us-west-2.amazonaws.com/{context.TrainingBucketName}/sagemaker/model/{context.TrainingJobName}/output/model.tar.gz"
                    }
                });

                context.ModelArn = result.ModelArn;
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        private sealed class CreateEndpointConfiguration : TaskState<Context, GetEndpoint>
        {
            readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateEndpointConfigAsync(new CreateEndpointConfigRequest
                {
                    EndpointConfigName = context.TrainingJobName,
                    ProductionVariants = new List<ProductionVariant>
                    {
                        new ProductionVariant
                        {
                            InstanceType = ProductionVariantInstanceType.MlM4Xlarge,
                            InitialVariantWeight = 1,
                            InitialInstanceCount = 1,
                            ModelName = context.TrainingJobName,
                            VariantName = "AllTraffic"
                        }
                    }
                });

                context.EndpointConfigArn = result.EndpointConfigArn;
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        private sealed class GetEndpoint : TaskState<Context, DetermineIfEndpointExists>
        {
            readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                try
                {
                    var result = await sageMaker.DescribeEndpointAsync(new DescribeEndpointRequest
                    {
                        EndpointName = context.SearchTerm
                    });

                    if (result.EndpointName == context.SearchTerm)
                        context.EndpointExists = true;
                    else context.EndpointExists = false;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    context.EndpointExists = false;
                }

                return context;
            }
        }

        private sealed class DetermineIfEndpointExists : ChoiceState
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<CreateEndpoint, Context>(c => c.EndpointExists == false),
                new Choice<UpdateEndpoint, Context>(c => c.EndpointExists == true)
            };
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        private sealed class CreateEndpoint : TaskState<Context, Done>
        {
            readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateEndpointAsync(new CreateEndpointRequest
                {
                    EndpointName = context.SearchTerm,
                    EndpointConfigName = context.TrainingJobName
                });

                context.EndpointArn = result.EndpointArn;

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        private sealed class UpdateEndpoint : TaskState<Context, Done>
        {
            readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.UpdateEndpointAsync(new UpdateEndpointRequest
                {
                    EndpointName = context.SearchTerm,
                    EndpointConfigName = context.TrainingJobName
                });

                context.EndpointArn = result.EndpointArn;

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class SubmitTrainingJob : TaskState<Context, CheckTrainingStatus>
        {
            readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                context.TrainingImage = "433757028032.dkr.ecr.us-west-2.amazonaws.com/xgboost:latest";
                context.TrainingRoleArn = context.TrainingRoleArn;
                context.TrainingJobName = $"{context.SearchTerm}-{context.QueryExecutionId}";

                var result = await sageMaker.CreateTrainingJobAsync(new CreateTrainingJobRequest
                {
                    TrainingJobName = context.TrainingJobName,
                    AlgorithmSpecification = new AlgorithmSpecification
                    {
                        TrainingInputMode = TrainingInputMode.File,
                        TrainingImage = context.TrainingImage
                    },
                    RoleArn = context.TrainingRoleArn,
                    OutputDataConfig = new OutputDataConfig
                    {
                        S3OutputPath = $"s3://{context.TrainingBucketName}/sagemaker/model"
                    },
                    ResourceConfig = new ResourceConfig
                    {
                        InstanceCount = 1,
                        InstanceType = TrainingInstanceType.MlM4Xlarge,
                        VolumeSizeInGB = 5
                    },
                    StoppingCondition = new StoppingCondition
                    {
                        MaxRuntimeInSeconds = 2000
                    },
                    HyperParameters = new Dictionary<string, string>
                    {
                        {"max_depth", "10"},
                        {"eta", "0.2"},
                        {"gamma", "4"},
                        {"min_child_weight", "1"},
                        {"subsample", "0.7"},
                        {"silent", "0"},
                        {"objective", "reg:linear"},
                        {"num_round", "50"}
                    },
                    InputDataConfig = new List<Channel>
                    {
                        new Channel
                        {
                            ChannelName = "train",
                            CompressionType = CompressionType.None,
                            ContentType = "csv",
                            DataSource = new DataSource
                            {
                                S3DataSource = new S3DataSource
                                {
                                    S3DataType = S3DataType.S3Prefix,
                                    S3DataDistributionType = S3DataDistribution.FullyReplicated,
                                    S3Uri = $"s3://{context.TrainingBucketName}/{context.TrainingKeyName}"
                                }
                            }
                        },
                        new Channel
                        {
                            ChannelName = "validation",
                            CompressionType = CompressionType.None,
                            ContentType = "csv",
                            DataSource = new DataSource
                            {
                                S3DataSource = new S3DataSource
                                {
                                    S3DataType = S3DataType.S3Prefix,
                                    S3DataDistributionType = S3DataDistribution.FullyReplicated,
                                    S3Uri = $"s3://{context.TrainingBucketName}/{context.ValidationKeyName}"
                                }
                            }
                        }
                    }
                });

                context.TrainingJobArn = result.TrainingJobArn;

                return context;
            }
        }

        [FunctionMemory(Memory = 512)]
        [FunctionTimeout(Timeout = 120)]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class StoreTrainingData : TaskState<Context, SubmitTrainingJob>
        {
            private readonly IAmazonS3 s3 = new AmazonS3Client();

            public override async Task<Context> Execute(Context context)
            {
                context.TrainingKeyName = "sagemaker/training.csv";
                context.ValidationKeyName = "sagemaker/validation.csv";

                var getResult = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = context.ResultsBucketName,
                    Key = $"{context.QueryExecutionId}.csv"
                });


                var reader = new StreamReader(getResult.ResponseStream);

                var csv = await reader.ReadToEndAsync();

                // TODO: randomize the order of the array

                var n = 2;
                var lines = csv
                    .Split(Environment.NewLine.ToCharArray())
                    .Skip(n)
                    .ToArray();

                var trainingLength = Convert.ToInt16(lines.Length * 0.7);
                var validationLength = Convert.ToInt16(lines.Length - trainingLength);

                var trainingSet = lines.Skip(0).Take(trainingLength);
                var validationSet = lines.Skip(trainingLength).Take(validationLength);

                var trainingCsv = string.Join(Environment.NewLine, trainingSet).Replace("\"", string.Empty);
                var validationCsv = string.Join(Environment.NewLine, validationSet).Replace("\"", string.Empty);

                var trainingUpload = s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = context.TrainingBucketName,
                    Key = context.TrainingKeyName,
                    ContentBody = trainingCsv,
                    ContentType = "text/csv"
                });

                var validationUpload = s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = context.TrainingBucketName,
                    Key = context.ValidationKeyName,
                    ContentBody = validationCsv,
                    ContentType = "text/csv"
                });

                await Task.WhenAll(trainingUpload, validationUpload);

                reader.Dispose();

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "athena:*")]
        [DotStep.Core.Action(ActionName = "glue:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class SubmitQuery : TaskState<Context, WaitForQueryToComplete>
        {
            readonly IAmazonAthena athena = new AmazonAthenaClient();
            readonly IAmazonGlue glue = new AmazonGlueClient();

            public override async Task<Context> Execute(Context context)
            {
                var sqlBuilder = new StringBuilder();
                sqlBuilder.AppendLine("SELECT (ix1.price - ix.price) / ix.price,");
                var itemTableDefinition =
                    await glue.GetTableAsync(new GetTableRequest {DatabaseName = "tiger", Name = context.TableName});
                var itemColumns = itemTableDefinition.Table.StorageDescriptor.Columns;
                var colIndex = 0;
                foreach (var column in itemColumns)
                {
                    colIndex++;
                    if (column.Name != "date" && column.Name != "urlmd5" && column.Name != "url")
                    {
                        var comma = colIndex < itemColumns.Count ? "," : string.Empty;
                        sqlBuilder.AppendLine($"COALESCE(\"{column.Name}\",0){comma}");
                    }
                }

                sqlBuilder.AppendLine($"FROM \"tiger\".\"{context.TableName}\" i");
                sqlBuilder.AppendLine(
                    $"JOIN \"tiger\".\"indexes\" ix ON ix.index = '{context.SearchTerm}' AND i.date = concat(ix.date, 'T00:00:00Z')");
                sqlBuilder.AppendLine(
                    $"JOIN \"tiger\".\"indexes\" ix1  ON ix1.index = '{context.SearchTerm}' AND date_add('day', 1, date(replace(i.date, 'T00:00:00Z', ''))) = date(ix1.date)");

                var sql = sqlBuilder.ToString();

                var result = await athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
                {
                    QueryString = sql,
                    QueryExecutionContext = new QueryExecutionContext
                    {
                        Database = "tiger"
                    },
                    ResultConfiguration = new ResultConfiguration
                    {
                        OutputLocation = $"s3://{context.ResultsBucketName}/"
                    }
                });

                context.QueryExecutionId = result.QueryExecutionId;

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "glue:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class ReCrawlEntities : TaskState<Context, WaitForCrawlingToComplete>
        {
            private readonly IAmazonGlue glue = new AmazonGlueClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await glue.StartCrawlerAsync(new StartCrawlerRequest
                {
                    Name = "tiger-entities"
                });
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "athena:*")]
        [DotStep.Core.Action(ActionName = "glue:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class CreateTable : TaskState<Context, SubmitQuery>
        {
            private readonly IAmazonAthena athena = new AmazonAthenaClient();
            private readonly IAmazonGlue glue = new AmazonGlueClient();

            public override async Task<Context> Execute(Context context)
            {
                //var r = await glue.GetTableAsync(new GetTableRequest { Name = "entities", DatabaseName = "tiger" });

                context.TableName = $"items-{DateTime.UtcNow.Ticks}";

                var result = athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
                {
                    QueryString =
                        $"SELECT at FROM tiger.entities WHERE st = '{context.SearchTerm}' AND at is not null GROUP BY at ORDER BY count(*) DESC LIMIT 998",
                    QueryExecutionContext = new QueryExecutionContext
                    {
                        Database = "tiger"
                    },
                    ResultConfiguration = new ResultConfiguration
                    {
                        OutputLocation = $"s3://{context.QueryExecutionBucket}/"
                    }
                });

                await Task.WhenAll(result, Task.Delay(TimeSpan.FromSeconds(15)));


                var columns = new List<Column>
                {
                    new Column
                    {
                        Name = "Date",
                        Type = "string"
                    },
                    new Column
                    {
                        Name = "UrlMD5",
                        Type = "string"
                    },
                    new Column
                    {
                        Name = "Url",
                        Type = "string"
                    }
                };

                var result2 = await athena.GetQueryResultsAsync(new GetQueryResultsRequest
                {
                    QueryExecutionId = result.Result.QueryExecutionId,
                    MaxResults = 998
                });

                // delete the first row becauase it's the column name
                result2.ResultSet.Rows.RemoveAt(0);

                foreach (var row in result2.ResultSet.Rows)
                    columns.Add(new Column
                    {
                        Name = $"{row.Data[0].VarCharValue}",
                        Type = "int"
                    });

                var serdePaths = string.Join(',', columns.Select(c => c.Name));

                var result3 = await glue.CreateTableAsync(new CreateTableRequest
                {
                    DatabaseName = "tiger",

                    TableInput = new TableInput
                    {
                        Name = context.TableName,
                        TableType = "EXTERNAL_TABLE",
                        Parameters = new Dictionary<string, string>
                        {
                            {"classification", "json"},
                            {"compressionType", "gzip"},
                            {"CrawlerSchemaSerializerVersion", "1.0"},
                            {"CrawlerSchemaDeserializerVersion", "1.0"},
                            {"typeOfData", "file"}
                        },

                        StorageDescriptor = new StorageDescriptor
                        {
                            Compressed = true,
                            Columns = columns,
                            Location = $"s3://{context.TrainingBucketName}/items/",
                            InputFormat = "org.apache.hadoop.mapred.TextInputFormat",
                            OutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                            SerdeInfo = new SerDeInfo
                            {
                                Parameters = new Dictionary<string, string>
                                {
                                    {"paths", serdePaths}
                                },
                                SerializationLibrary = "org.openx.data.jsonserde.JsonSerDe"
                            }
                        }
                    }
                });

                return context;
            }
        }
    }
}