using System.IO;

namespace ContinuousTraining.Formatting
{
    public interface IDataFormatter
    {
        string ContentType { get; }

        // using streams instead of strings so we can support larger data sizes. 
        void ProcessData(Stream input, out Stream training, out Stream validation);
    }


}