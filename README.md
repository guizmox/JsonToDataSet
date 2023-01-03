# JsonToDataSet
This is a C# Json Parser that converts Json data as a DataSet

Demo : 

using JsonToDataSet;
using System.Data;
using System.IO;

namespace ConsoleApp1
{
  string sFile = @"C:\Tools\myJsonFile.js";
  string sJsonData = File.ReadAllText(sFile);

  JsonParser jsp = new JsonParser(sJsonData, "MyJsonData");
  jsp.MultiThreadComputation = true;
  DataSet dsResult = jsp.JsonToDataSet();
}
