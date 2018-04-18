using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ContinuousTraining.Formatting
{
    public class LibsvmFormatter : StringDataFormatter
    {
        public override string ContentType => "text/x-libsvm";

        public override void ProcessData(List<string> trainingSet, List<string> validationSet,
            out string training, out string validation)
        {
            training = ToLibSvm(trainingSet);
            validation = ToLibSvm(validationSet);
        }

        public string ToLibSvm(List<string> inputSet, bool includePrediction = true)
        {
            var sb = new StringBuilder();

            foreach (var row in inputSet)
            {
                var values = row.Replace("\"", string.Empty).Split(',');

                //if (includePrediction)
                //{
                    var predictionValue = values[0];

                    if (predictionValue.Length <= 8) continue;

                    sb.Append(predictionValue.Substring(0, 8));
                    values = values.Skip(1).ToArray();
                //}
                
                for (var i = 0; i < values.Length; i++)
                {
                    var value = values[i];
                    if (value != "0")
                        sb.Append($" {i + 1}:{value}");
                }

                sb.AppendLine();
            }

            return sb.ToString().Trim();
        }
    }
}