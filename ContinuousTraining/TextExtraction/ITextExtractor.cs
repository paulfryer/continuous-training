using System;
using System.Threading.Tasks;

namespace ContinuousTraining.TextExtraction
{
    public interface ITextExtractor
    {
        Task<string> ExtractText(Uri url);
    }
}