using System;
using System.Linq;
using System.Threading.Tasks;
using ContinuousTraining.EntityExtraction;
using ContinuousTraining.EntityExtraction.ContinuousTraining.EntityExtractors;
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
        public async Task ExtractionStateMachine()
        {
            var context = new Extractor.Context
            {
                SearchTerm = "Amazon Web Services",
                SearchDate = DateTime.UtcNow
            };

            var engine = new StateMachineEngine<Extractor, Extractor.Context>(context);

            await engine.Start();

            Assert.Inconclusive("Need to finish this test, determine what the success criteria is.");
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
                var result = await extractor.ExtractText(new Uri("https://www.amazon.com"));
                Assert.IsTrue(result.Length > 0);
            }
        }
    }
}