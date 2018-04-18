using System;
using System.Collections.Generic;

namespace ContinuousTraining.Formatting
{
    public class CsvFormatter : StringDataFormatter
    {
        public override string ContentType => "text/csv";

        public override void ProcessData(List<string> trainingSet, List<string> validationSet,
            out string training, out string validation)
        {
            training = string.Join(Environment.NewLine, trainingSet).Replace("\"", string.Empty);
            validation = string.Join(Environment.NewLine, validationSet).Replace("\"", string.Empty);
        }
    }
}