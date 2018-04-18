using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ContinuousTraining.Formatting
{
    // Use this if all your data fits in a single string (within Lambda memory sizes). If not, you need to implement the stream interface.

    public abstract class StringDataFormatter : IDataFormatter
    {
        public void ProcessData(Stream input, out Stream training, out Stream validation)
        {
            var reader = new StreamReader(input);
            var stringData = reader.ReadToEndAsync().Result;

            // TODO: randomize the order of the array
            var n = 1;
            var lines = stringData
                .Split(Environment.NewLine.ToCharArray())
                .Skip(n)
                .ToArray();

            var trainingLength = Convert.ToInt16(lines.Length * 0.7);
            var validationLength = Convert.ToInt16(lines.Length - trainingLength);

            var trainingSet = lines.Skip(0).Take(trainingLength).ToList();
            var validationSet = lines.Skip(trainingLength).Take(validationLength).ToList();

            ProcessData(trainingSet, validationSet, out var trainingData, out var validationData);
            training = StringToStream(trainingData);
            validation = StringToStream(validationData);
        }

        public abstract string ContentType { get; }

        public abstract void ProcessData(List<string> trainingSet, List<string> validationSet,
            out string training, out string validation);

        public Stream StringToStream(string stringData)
        {
            var uniEncoding = new UnicodeEncoding();
            var ms = new MemoryStream();
            var sw = new StreamWriter(ms, uniEncoding);
            sw.Write(stringData);
            sw.Flush();
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }
    }
}