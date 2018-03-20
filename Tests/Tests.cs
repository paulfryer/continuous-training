using System;
using System.Linq;
using System.Threading.Tasks;
using ContinuousTraining.EntityExtraction;
using ContinuousTraining.EntityExtraction.ContinuousTraining.EntityExtractors;
using ContinuousTraining.TextExtraction;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tests
{
    [TestClass]
    public class Tests
    {
        [TestMethod]
        public async Task TestAmazon()
        {
            IEntityExtractor extractor = new AmazonEntityExtractor();
            var result = await extractor.ExtractEntitiesAsync("The guy named Joe Thomas won the race and lives in Seattle.");
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
