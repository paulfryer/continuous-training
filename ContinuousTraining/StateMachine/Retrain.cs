﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Glue;
using Amazon.Glue.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SageMaker;
using Amazon.SageMaker.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using ContinuousTraining.Formatting;
using DotStep.Common.Functions;
using DotStep.Core;
using CreateTableRequest = Amazon.Glue.Model.CreateTableRequest;

namespace ContinuousTraining.StateMachine
{
    public sealed class Retrain : StateMachine<Retrain.SetDefaults>
    {
        public class Context : IContext
        {
            [Required] public string SearchTerm { get; set; }

            [Required] public string Symbol { get; set; }

            public string TableName { get; set; }

            public string QueryExecutionId { get; set; }

            [Required] public string ResultsBucketName { get; set; }

            [Required] public string TrainingBucketName { get; set; }

            public string TrainingKeyName { get; set; }
            public string ValidationKeyName { get; set; }

            public string TrainingJobName { get; set; }
            public string TrainingImage { get; set; }

            [Required] public string TrainingRoleArn { get; set; }

            public string TrainingJobArn { get; set; }
            public string TrainingJobStatus { get; set; }

            public string ModelArn { get; set; }

            public string EndpointConfigArn { get; set; }

            public bool EndpointExists { get; set; }

            public string EndpointArn { get; set; }

            [Required] public string QueryExecutionBucket { get; set; }

            [Required] public string DataFormat {get;set;}
        }

        [DotStep.Core.Action(ActionName = "ssm:*")]
        public sealed class SetDefaults : TaskState<Context, Validate>
        {
            private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await ssm.GetParametersAsync(new GetParametersRequest
                {
                    Names = new List<string>
                    {
                        "/CT/BucketName",
                        "/CT/SageMakerRole",
                        "/CT/SageMakerTrainingContainer"
                    }
                });

                var defaultBucketName = result.Parameters.Single(p => p.Name == "/CT/BucketName").Value;
                var sageMakerRole = result.Parameters.Single(p => p.Name == "/CT/SageMakerRole").Value;
                var sageMakerContainer =
                    result.Parameters.Single(p => p.Name == "/CT/SageMakerTrainingContainer").Value;

                if (string.IsNullOrEmpty(context.QueryExecutionBucket))
                    context.QueryExecutionBucket = defaultBucketName;

                if (string.IsNullOrEmpty(context.ResultsBucketName))
                    context.ResultsBucketName = defaultBucketName;

                if (string.IsNullOrEmpty(context.TrainingBucketName))
                    context.TrainingBucketName = defaultBucketName;
                if (string.IsNullOrEmpty(context.TrainingRoleArn))
                    context.TrainingRoleArn = sageMakerRole;
                if (string.IsNullOrEmpty(context.TrainingImage))
                    context.TrainingImage = sageMakerContainer;

                if (string.IsNullOrEmpty(context.DataFormat))
                    context.DataFormat = "libsvm";

                return context;
            }
        }

        public sealed class Validate : ReferencedTaskState<Context, CrawlS3ForNewPartitions,
            ValidateMessage<Context>>
        {
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
        public sealed class RegisterModel : TaskState<Context, CreateEndpointConfiguration>
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
        public sealed class CreateEndpointConfiguration : TaskState<Context, GetEndpoint>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

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
        public sealed class GetEndpoint : TaskState<Context, DetermineIfEndpointExists>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                try
                {
                    var result = await sageMaker.DescribeEndpointAsync(new DescribeEndpointRequest
                    {
                        EndpointName = context.Symbol
                    });

                    if (result.EndpointName == context.Symbol)
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

        public sealed class DetermineIfEndpointExists : ChoiceState
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
        public sealed class CreateEndpoint : TaskState<Context, Done>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateEndpointAsync(new CreateEndpointRequest
                {
                    EndpointName = context.Symbol,
                    EndpointConfigName = context.TrainingJobName
                });

                context.EndpointArn = result.EndpointArn;

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class UpdateEndpoint : TaskState<Context, Done>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.UpdateEndpointAsync(new UpdateEndpointRequest
                {
                    EndpointName = context.Symbol,
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
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                context.TrainingRoleArn = context.TrainingRoleArn;
                context.TrainingJobName = $"{context.Symbol}-{context.QueryExecutionId}";

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
                        MaxRuntimeInSeconds = Convert.ToInt32(TimeSpan.FromHours(1).TotalSeconds)
                    },
                    HyperParameters = new Dictionary<string, string>
                    {
                        {"max_depth", "6"},
                        {"eta", "0.05"},
                        {"min_child_weight", "3"},
                        {"subsample", "0.8"},
                        {"silent", "0"},
                        {"objective", "reg:linear"},
                        {"num_round", "200"}
                    },
                    InputDataConfig = new List<Channel>
                    {
                        new Channel
                        {
                            ChannelName = "train",
                            CompressionType = CompressionType.None,
                            ContentType = context.DataFormat,
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
                            ContentType = context.DataFormat,
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

        [FunctionMemory(Memory = 1024)]
        [FunctionTimeout(Timeout = 240)]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class StoreTrainingData : TaskState<Context, SubmitTrainingJob>
        {
            private readonly IAmazonS3 s3 = new AmazonS3Client();
            
            private IDataFormatter formatter;

            public override async Task<Context> Execute(Context context)
            {
                switch (context.DataFormat)
                {
                    case "libsvm":
                        formatter = new LibsvmFormatter();
                        break;
                    case "csv":
                        formatter = new CsvFormatter();
                        break;
                    default:
                        throw new Exception($"Unsupported data format: {context.DataFormat}.");
                }

                context.TrainingKeyName = $"sagemaker/training.{context.DataFormat}";
                context.ValidationKeyName = $"sagemaker/validation.{context.DataFormat}";

                var getResult = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = context.ResultsBucketName,
                    Key = $"{context.QueryExecutionId}.csv"
                });
               
                formatter.ProcessData(getResult.ResponseStream, out var training, out var validation);

                var trainingUpload = s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = context.TrainingBucketName,
                    Key = context.TrainingKeyName,
                    InputStream = training,
                    ContentType = formatter.ContentType
                });

       

                var validationUpload = s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = context.TrainingBucketName,
                    Key = context.ValidationKeyName,
                    InputStream = validation,
                    ContentType = formatter.ContentType
                });

                await Task.WhenAll(trainingUpload, validationUpload);

                training.Dispose();
                validation.Dispose();

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "athena:*")]
        [DotStep.Core.Action(ActionName = "glue:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class SubmitQuery : TaskState<Context, WaitForQueryToComplete>
        {
            private readonly IAmazonAthena athena = new AmazonAthenaClient();
            private readonly IAmazonGlue glue = new AmazonGlueClient();

            public override async Task<Context> Execute(Context context)
            {
                // TODO: need to build a price / index refernce import process. Start with yahoo finance.

                var sqlBuilder = new StringBuilder();
                sqlBuilder.AppendLine("SELECT (ix1.close - ix.close) / ix.close,");
                var itemTableDefinition =
                    await glue.GetTableAsync(new GetTableRequest
                    {
                        DatabaseName = "extraction-database",
                        Name = context.TableName
                    });
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

                sqlBuilder.AppendLine($"FROM \"extraction-database\".\"{context.TableName}\" i");
                sqlBuilder.AppendLine(
                    $"JOIN \"extraction-database\".\"index\" ix ON ix.symbol = '{context.Symbol}' AND i.date = concat(ix.date, 'T00:00:00Z')");
                sqlBuilder.AppendLine(
                    $"JOIN \"extraction-database\".\"index\" ix1  ON ix1.symbol = '{context.Symbol}' AND date_add('day', 1, date(replace(i.date, 'T00:00:00Z', ''))) = date(ix1.date)");

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
        public sealed class CrawlS3ForNewPartitions : TaskState<Context, WaitForCrawlingToComplete>
        {
            private readonly IAmazonGlue glue = new AmazonGlueClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await glue.StartCrawlerAsync(new StartCrawlerRequest
                {
                    Name = "ingest-crawler"
                });
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "athena:*")]
        [DotStep.Core.Action(ActionName = "glue:*")]
        [DotStep.Core.Action(ActionName = "dynamodb:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class CreateTable : TaskState<Context, SubmitQuery>
        {
            private readonly IAmazonAthena athena = new AmazonAthenaClient();
            private readonly IAmazonDynamoDB dynamo = new AmazonDynamoDBClient();
            private readonly IAmazonGlue glue = new AmazonGlueClient();

            public override async Task<Context> Execute(Context context)
            {
                //var r = await glue.GetTableAsync(new GetTableRequest { Name = "entities", DatabaseName = "tiger" });

                context.TableName = $"items-{DateTime.UtcNow.Ticks}";

                var result = athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
                {
                    QueryString =
                        $"SELECT at FROM \"entity\" WHERE st = '{context.SearchTerm}' AND at is not null GROUP BY at ORDER BY count(*) DESC LIMIT 1000",
                    QueryExecutionContext = new QueryExecutionContext
                    {
                        Database = "extraction-database"
                    },
                    ResultConfiguration = new ResultConfiguration
                    {
                        OutputLocation = $"s3://{context.QueryExecutionBucket}/athena/"
                    }
                });

                await Task.WhenAll(result, Task.Delay(TimeSpan.FromSeconds(15)));

                var latestPredictionProperties = new List<string>();

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
                    MaxResults = 1000
                });

                // delete the first row becauase it's the column name
                result2.ResultSet.Rows.RemoveAt(0);

                foreach (var row in result2.ResultSet.Rows)
                {
                    var columnName = row.Data[0].VarCharValue;
                    latestPredictionProperties.Add(columnName);
                    columns.Add(new Column
                    {
                        Name = columnName,
                        Type = "int"
                    });
                }

                var serdePaths = string.Join(',', columns.Select(c => c.Name));

                var createTableTask = glue.CreateTableAsync(new CreateTableRequest
                {
                    DatabaseName = "extraction-database",

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
                            Location = $"s3://{context.TrainingBucketName}/ingest/item/",
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
                var putTask = dynamo.PutItemAsync(new PutItemRequest
                {
                    TableName = "symbol-schema",
                    Item = new Dictionary<string, AttributeValue>
                    {
                        {
                            "symbol", new AttributeValue
                            {
                                S = context.Symbol
                            }
                        },
                        {
                            "schema", new AttributeValue
                            {
                                SS = latestPredictionProperties
                            }
                        }
                    }
                });

                await Task.WhenAll(createTableTask, putTask);

                return context;
            }
        }
    }
}