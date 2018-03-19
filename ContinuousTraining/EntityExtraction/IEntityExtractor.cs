using System.Collections.Generic;
using System.Threading.Tasks;

namespace ContinuousTraining.EntityExtraction
{
    public interface IEntityExtractor
    {
        string ProviderCode { get; }
        Task<List<ExtractedEntity>> ExtractEntitiesAsync(string text);
    }
}