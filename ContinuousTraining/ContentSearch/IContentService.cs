using System;
using System.Threading.Tasks;

namespace ContinuousTraining.ContentSearch
{
    public interface IContentService
    {
        Task<SearchResult> GetLinksAsync(string searchTerm, DateTime date, int startIndex);
    }
}