# JsonToDataSet
This is a C# Json Parser that converts Json data as a DataSet

Demo : 

using JsonToDataSet;
using System.Data;
using System.IO;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            // @"C:\Users\Public\Documents\Fuzible\FILES\direct-messages.js"
            string sFile = @"C:\Users\Public\Documents\Fuzible\FILES\ad-engagements.js";
            string cLoutilJson = File.ReadAllText(sFile);

            JsonParser jsp = new JsonParser(cLoutilJson, "leBonOutil");
            jsp.MultiThreadComputation = true;
            DataSet dsLoutil = jsp.JsonToDataSet();

        }
    }
}
