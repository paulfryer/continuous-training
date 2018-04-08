using System;
using System.Linq;
using System.Threading.Tasks;
using ContinuousTraining.EntityExtraction;
using ContinuousTraining.EntityExtraction.ContinuousTraining.EntityExtractors;
using ContinuousTraining.Indexing;
using ContinuousTraining.StateMachine;
using ContinuousTraining.TextExtraction;
using DotStep.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tests
{
    [TestClass]
    public class StateMachines
    {
        [TestMethod]
        public void EnsureStateMachineJsonIsGenerated()
        {
            IStateMachine stateMachine = new Extractor();

            var description = stateMachine.Describe("us-west-2", "0123456789");

            Assert.IsNotNull(description);
        }


        [TestMethod]
        public async Task RetrainStateMachine()
        {
            var context = new Retrain.Context
            {
                SearchTerm = "YouTube",
                TrainingBucketName = "us-west-2-ct900-989469592528",
                ResultsBucketName = "us-west-2-ct900-989469592528",
                QueryExecutionBucket = "us-west-2-ct900-989469592528"
            };

            var engine = new StateMachineEngine<Retrain, Retrain.Context>(context);

            await engine.Start();

            // if we made it this far, it worked.
            Assert.IsTrue(true);
        }

        [TestMethod]
        public async Task ExtractionStateMachine()
        {
            var context = new Extractor.Context
            {
                SearchTerm = "YouTube",
                SearchDate = DateTime.UtcNow.Subtract(TimeSpan.FromDays(2))
            };

            var engine = new StateMachineEngine<Extractor, Extractor.Context>(context);

            await engine.Start();

            // if we made it this far, it worked.
            Assert.IsTrue(true);
        }


        [TestClass]
        public class IndexingServices
        {

            [TestMethod]
            public async Task TestAlphaVantage()
            {
                var symbol = "AMZN";
                var start = DateTime.UtcNow;
                var end = DateTime.UtcNow.Subtract(TimeSpan.FromDays(7));
                IIndexingService yahooFinanceService = new AlphaVantageIndexingService();
                var stats = await yahooFinanceService.GetStatisticsAsync(start, end, symbol);
                Assert.IsTrue(stats.Any());
            }


            [TestMethod]
            public async Task TestYahooFinance()
            {
                var symbol = "AMZN";
                var start = DateTime.UtcNow;
                var end = DateTime.UtcNow.Subtract(TimeSpan.FromDays(7));
                IIndexingService yahooFinanceService = new YahooFinanceIndexingService();
                var stats = await yahooFinanceService.GetStatisticsAsync(start, end, symbol);
                Assert.IsTrue(stats.Any());
            }



        }

        [TestClass]
        public class ExtractionServices
        {
            [TestMethod]
            public async Task TestAmazonEntityExtraction()
            {
                IEntityExtractor extractor = new AmazonEntityExtractor();
                var result =
                    await extractor.ExtractEntitiesAsync("The guy named Joe Thomas won the race and lives in Seattle.");
                Assert.IsTrue(result.Any(i => i.Name == "Joe Thomas" && i.Type == "PERSON"));
                Assert.IsTrue(result.Any(i => i.Name == "Seattle" && i.Type == "LOCATION"));
            }


            [TestMethod]
            public async Task TestDiffbot()
            {
                ITextExtractor extractor = new DiffbotTextExtractor();
                var result = await extractor.ExtractText(new Uri("http://www.cnn.com"));
                Assert.IsTrue(result.Length > 0);
            }
        }
    }
}