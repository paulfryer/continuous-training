using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HtmlAgilityPack;

namespace ContinuousTraining.TextExtraction
{
    public class HtmlAgilityExtractor : ITextExtractor {

        public async Task<string> ExtractText(Uri url)
        {
            
            var web = new HtmlWeb();
            var doc = web.Load(url);

            StringBuilder sb = new StringBuilder();
            IEnumerable<HtmlNode> nodes = doc.DocumentNode.Descendants().Where(n =>
                n.NodeType == HtmlNodeType.Text &&
                n.ParentNode.Name != "script" &&
                n.ParentNode.Name != "style");
            foreach (HtmlNode node in nodes)
            {
                //Console.WriteLine(node.InnerText);
                sb.Append(node.InnerText);
            }

            return sb.ToString();

            //return doc.DocumentNode.SelectSingleNode("//body").InnerText;
        }

    }
}