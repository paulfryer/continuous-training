namespace ContinuousTraining.EntityExtraction
{
    public class ExtractedEntity
    {
        public ExtractedEntity()
        {
        }

        public ExtractedEntity(string name, string type, int score)
        {
            Name = name;
            Type = type;
            Score = score;
        }

        public string Name { get; set; }
        public string Type { get; set; }
        public int Score { get; set; }
    }
}