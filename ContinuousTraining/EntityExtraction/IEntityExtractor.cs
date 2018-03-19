using System.Collections.Generic;
using System.Threading.Tasks;

namespace ContinuousTraining.EntityExtraction
{
    public interface IEntityExtractor
    {
        Task<List<ExtractedEntity>> ExtractEntitiesAsync(string text);
    }
}