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
            string sFile = @"C:\TOOLS\myJsonFile.js";
            string sJsonData = File.ReadAllText(sFile);
            JsonParser jsp = new JsonParser(sJsonData, "MyFile");
            jsp.MultiThreadComputation = true;
            DataSet dsResult = jsp.JsonToDataSet();
        }
    }
}
