using System;
using System.Collections.Generic;

namespace ContinuousTraining.ContentSearch
{
    public class SearchResult
    {
        public bool HasMoreResults { get; set; }
        public List<Uri> Links { get; set; }
    }
}