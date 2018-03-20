namespace ContinuousTraining.EntityExtraction
{
    public class ExtractedEntity
    {
        public ExtractedEntity()
        {
        }

        public ExtractedEntity(string name, string type, int score, string provider)
        {
            Name = name;
            Type = type;
            Score = score;
            Provider = provider;
        }

        public string Name { get; set; }
        public string Type { get; set; }
        public int Score { get; set; }
        public string Provider { get; set; }
    }
}