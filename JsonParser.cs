using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace JsonToDataSet
{
    public class JsonParser
    {
        #region GLOBAL_VARIABLES

        DataSet dsJson = null;

        #endregion

        #region EVENTS

        /// <summary>
        /// Event that can be handled to get Processing Information during the Parsing
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="sInfo"></param>
        public delegate void JsonParserInfo(object sender, string sInfo);
        public event JsonParserInfo OnJsonEvent;

        #endregion

        #region PRIVATE VARIABLES

        private const string REGEX_JSON_TABLE = @"^(.+)(\{\[\d+\]\})$";
        private string _sJson = "";
        private readonly string _sOutput = "";
        private readonly string _sJsonPrefix = "";
        private bool Processing { get; set; } = false;

        #endregion

        #region PUBLIC VARIABLES

        /// <summary>
        /// Indicates if the provided Json Data is valid
        /// </summary>
        public bool IsValid { get; } = true;
        /// <summary>
        /// If set to True, Data will be interpreted as Bson Data
        /// </summary>
        public bool IsBson { get; set; } = false;
        /// <summary>
        /// Returns if the parsing was successful
        /// </summary>
        public bool Success { get; private set; } = false;
        /// <summary>
        /// If provided, a column can be set as a Primary Key for parsed data
        /// </summary>
        public string ForcePrimaryKey { get; set; } = "";
        /// <summary>
        /// If provided, will limit Json analysis to the definied depth.
        /// </summary>
        public int DepthToGet { get; set; } = 0;
        /// <summary>
        /// If set to True, column names will be renammed be replacing special characters by an underscore
        /// </summary>
        public bool AvoidSpecialCharsInColumnNames { get; set; } = true;
        /// <summary>
        /// If set to True, Array data will be parsed as datatables with a strong relationship with the upper level.
        /// </summary>
        public bool ArraysAsNewTables { get; set; } = false;
        /// <summary>
        /// If set to True, while parsing Json data, Primary keys may be set to create relationship between DataTables.
        /// </summary>
        public bool RemovePrimaryKey { get; set; } = true;
        /// <summary>
        /// Force another CultureInfo to be used (this is used by the parser to identity numeric data) 
        /// </summary>
        public static CultureInfo CultureInf { get; set; } = CultureInfo.GetCultureInfoByIetfLanguageTag(CultureInfo.CurrentCulture.IetfLanguageTag);

        /// <summary>
        /// Is set to True, Parsing will be multithreaded, which should improve speed on large Json data
        /// </summary>
        public bool MultiThreadComputation { get; set; } = true;

        #endregion

        #region PUBLIC METHODS

        /// <summary>
        /// Initialize the Parser
        /// </summary>
        /// <param name="sJson">Json string</param>
        /// <param name="sOutput">DataSet Name and DataTable(s) prefix</param>
        public JsonParser(string sJson, string sOutput)
        {
            _sJson = DecodeEncodedNonAsciiCharacters(sJson).Trim();
            _sOutput = sOutput.Length == 0 ? "json" : sOutput;
            bool bStart = Regex.IsMatch(_sJson, "^({|\\[)");

            if (!bStart) //cas des fichiers JS de Twitter par exemple : window.YTD.tweets.part0 = [
            {
                bStart = Regex.IsMatch(_sJson, "^(.*[^{|\\[])(\\s?=\\s?)({|\\[)");

                if (bStart)
                {
                    var reg = Regex.Match(_sJson, "^(.*[^{|\\[])(\\s?=\\s?)({|\\[)");
                    _sJsonPrefix = reg.Groups[1].Value;
                }
            }

            bool bEnd = Regex.IsMatch(_sJson, "(}|\\])$");
            IsValid = bStart && bEnd;
        }

        /// <summary>
        /// Sync method that returns the parsed Json string as a full DataSet
        /// </summary>
        /// <returns></returns>
        public DataSet JsonToDataSet()
        {
            DateTime dtStart = DateTime.Now;
            try
            {
                LoadJson(new CancellationToken());
            }
            catch (Exception)
            {
                SetDataSetError(true);
            }
            DateTime dtStop = DateTime.Now;

            string sDiffCompute = (dtStop - dtStart).TotalSeconds.ToString();

            dtStart = DateTime.Now;

            if (Success) { CleanupDataSet(); }

            if (!Success) { SetDataSetError(true); }

            dtStop = DateTime.Now;

            string sDiffDataSet = (dtStop - dtStart).TotalSeconds.ToString();

            return dsJson;
        }

        /// <summary>
        /// Async method that returns the parsed Json string as a full DataSet
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<DataSet> JsonToDataSetAsync(CancellationToken cancellationToken)
        {
            dsJson = new DataSet(_sJsonPrefix.Length == 0 ? "SHSJsonParser" : _sJsonPrefix);

            return Task<DataSet>.Factory.StartNew(() =>
            {
                try
                {
                    dsJson = LoadJson(cancellationToken);
                }
                catch (OperationCanceledException ex)
                {
                    if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Operation Cancelled (" + ex.Message + ")"); }

                    while (Processing) { Thread.Sleep(100); }
                    SetDataSetError(false);
                }
                catch { }

                if (Success)
                {
                    CleanupDataSet();
                }

                return dsJson;
            }, cancellationToken);
        }

        #endregion

        #region PRIVATE METHODS

        private DataSet LoadJson(CancellationToken cancellationToken)
        {
            Processing = true;

            if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Starting Json To DataSet Process (Length : " + _sJson.Length.ToString() + ")"); }

            if (OnJsonEvent != null) { OnJsonEvent(this, IsValid ? "\t * Json Data is valid : " + _sOutput : "\t * Json Data is not valid ! " + _sOutput); }

            dsJson = new DataSet(_sJsonPrefix.Length == 0 ? "SHSJsonParser" : _sJsonPrefix);

            //int i1 = Regex.Matches(_sJson, "\\[").Count;
            //int i2 = Regex.Matches(_sJson, "\\]").Count;
            //int i3 = Regex.Matches(_sJson, "\\{").Count;
            //int i4 = Regex.Matches(_sJson, "\\}").Count;

            //if (i1 == i2 && i3 == i4)
            //{
            int iT = -1;

            //cleanup
            //_sJson = _sJson.Trim();

            //traitement particulier des documents BSON (MongoDB)
            if (IsBson)
            {
                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Bson Data detected, cleaning it up..."); }
                //new JsonWriterSettings { OutputMode = JsonOutputMode.Strict }
                _sJson = Regex.Replace(_sJson, @"(ObjectId\()(\"")([a-zA-Z0-9]+)(\"")(\))", "\"$3\"");
                //new JsonWriterSettings { OutputMode = JsonOutputMode.RelaxedExtendedJson }
                _sJson = Regex.Replace(_sJson, @"{\s{1}""\$oid""\s{1}:\s{1}(""[a-zA-Z0-9_]+"")\s{1}}", "$1");
            }

            if (IsValid) //valeur arbitraire de contrôle qu'il y a bien quelque chose à parser
            {
                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Parsing Json Data..."); }

                List<JsonPattern> jsPatterns = ParseJsonString(cancellationToken);

                int iLevels = 0;
                //recherche des niveaux 
                jsPatterns = jsPatterns.OrderBy(jsP => jsP.Depth).ToList();
                iLevels = jsPatterns.Last().Depth;

                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Json Data successfully Parsed : " + (iLevels + 1).ToString() + " level(s) detected."); }
                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Found " + jsPatterns.Count().ToString() + " chunk(s)."); }

                List<string> sParentNodes = new List<string>();
                DataSet dsData = null;

                for (int i = 0; i <= iLevels; i++)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        Processing = false;
                        Success = false;
                        throw new Exception("Cancellation Requested");
                    }

                    if (DepthToGet > 0) { if (i > (DepthToGet - 1)) { break; } }

                    var sJsonParent = jsPatterns.Where(jSP => jSP.Depth == i).ToList();

                    //multithread...
                    if (sJsonParent.Count > 0)
                    {
                        if (sParentNodes.Count > 1) { if (sParentNodes[1].Equals("q2")) { sParentNodes[1] = "q1"; } }
                        int number = sJsonParent.Count;
                        int numParts = MultiThreadComputation ? Environment.ProcessorCount : 1;
                        int partSize = number / numParts;
                        int remainder = number % numParts;

                        List<DataSet> dsList = new List<DataSet>();
                        List<Task> lTasks = new List<Task>();

                        for (int iR = 0; iR < numParts; iR++)
                        {
                            int iStart = iR * partSize;
                            int iCount = (iR == numParts - 1 ? partSize + remainder : partSize);
                            var JsPatterns = sJsonParent.GetRange(iStart, iCount);

                            List<string> sNodesInList = new List<string>();
                            foreach (var pat in JsPatterns)
                            {
                                if (!sNodesInList.Contains(pat.NodeName))
                                {
                                    sNodesInList.Add(pat.NodeName);
                                }
                            }
                            if (OnJsonEvent != null) { OnJsonEvent(this, "\t * [Thread ]" + iR.ToString() + "] Processing " + JsPatterns.Count().ToString() + " chunk(s) on Level " + (i + 1).ToString() + " (" + string.Join(",", sNodesInList) + ")"); }
                            //on attribue le bon node parent au niveau scanné
                            sParentNodes.Clear();
                            for (int iJ = 0; iJ < JsPatterns.Count; iJ++)
                            {
                                if (dsData != null)
                                {
                                    sParentNodes.Add(dsData.Tables[JsPatterns[iJ].ParentElement - 1].TableName);
                                }
                                else { sParentNodes.Add(""); }
                            }

                            lTasks.Add(Task.Factory.StartNew(() => dsList.Add(CreateDataTableFromJsonPatterns(JsPatterns, iStart, iLevels, sParentNodes, dsJson, cancellationToken))));
                        }

                        while (lTasks.Count(t => t.IsCompleted) < lTasks.Count)
                        {
                            Thread.Sleep(100);
                        }
                        //mixer les dsList
                        int iDs = 0;
                        foreach (DataSet ds in dsList.Cast<DataSet>().OrderBy(dsName => dsName.DataSetName))
                        {
                            if (iDs == 0)
                            { dsData = ds; }
                            else
                            {
                                foreach (DataTable dt in ds.Tables) //leBonOutil{[X]}
                                {
                                    dsData.Tables.Add(dt.Copy());
                                }
                            }
                            iDs++;
                        }
                        dsList.Clear();
                        dsList = null;

                        if (dsData.Tables.Count > 0)
                        {
                            foreach (DataTable dt in dsData.Tables)
                            {
                                iT++;
                                if (dsJson.Tables.Contains(dt.TableName))
                                {
                                    string sDtName = string.Concat(dt.TableName, "_", iT.ToString());

                                    //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Renaming DataTable '" + dt.TableName + "' -> '" + sDtName + "'");

                                    dt.TableName = sDtName;
                                }
                                if (dt.Rows.Count > 0)
                                {
                                    //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding DataTable '" + dt.TableName + "' to DataSet");

                                    dsJson.Tables.Add(dt.Copy());
                                }
                            }
                        }
                    }
                }
                Success = true;
            }
            else
            {
                SetDataSetError(true);
            }

            Processing = false;

            return dsJson;
        }

        private void SetDataSetError(bool bCreateRaw)
        {
            dsJson.Clear();
            dsJson.Dispose();
            dsJson = new DataSet();
            if (bCreateRaw)
            {
                dsJson.Tables.Add("result");
                dsJson.Tables[0].Columns.Add("error");
                DataRow dr = dsJson.Tables[0].NewRow();
                dr[0] = _sJson;
                dsJson.Tables[0].Rows.Add(dr);
                Success = false;
            }
        }

        private void CleanupDataSet()
        {
            if (OnJsonEvent != null) { OnJsonEvent(this, "\t * DataSet Consolidation (" + dsJson.Tables.Count.ToString() + " table(s))"); }

            Success = false;
            //}
            //merge des datatables avec des noms aux racines communes
            List<string> sMergedNames = new List<string>();
            List<List<string>> sMergedList = new List<List<string>>();
            foreach (DataTable dt in dsJson.Tables)
            {
                string sTA = Regex.Replace(dt.TableName, REGEX_JSON_TABLE, "$1");
                if (!sMergedNames.Contains(sTA))
                {
                    sMergedNames.Add(sTA); sMergedList.Add(new List<string>());
                }
            }

            for (int i = 0; i < sMergedNames.Count; i++)
            {
                foreach (DataTable dt in dsJson.Tables)
                {
                    string sTB = Regex.Replace(dt.TableName, REGEX_JSON_TABLE, "$1");
                    if (sTB.Equals(sMergedNames[i]))
                    {
                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Preparing Merging of '" + dt.TableName + "' With '" + sMergedList[i] + "'");

                        sMergedList[i].Add(dt.TableName);
                    }
                }
            }

            //----------------------------------------------------------------------------------------------------------------------------------------------------ULTRA LONG
            //je me retrouve donc avec une liste contenant les différentes listes de tables à merger
            List<string> sListMerge = new List<string>();
            foreach (List<string> sL in sMergedList)
            {
                if (sL.Count > 0) { sListMerge.Add(string.Concat(sL[0], " : ", sL.Count.ToString(), " table(s)")); }
            }
            if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Merging " + dsJson.Tables.Count + " Table(s) From List (" + string.Join(",", sListMerge) + ")"); }

            for (int i = 0; i < sMergedList.Count; i++)
            {
                //découplage multithread : on a une quantité de tables à merger, on répartit cette quantité en fonction du nombre de threads
                int number = sMergedList[i].Count;
                int numParts = MultiThreadComputation ? Environment.ProcessorCount : 1;
                int partSize = number / numParts;
                int remainder = number % numParts;

                List<Task> lTasks = new List<Task>();
                var sListDTToMerge = new List<string>();
                var sListDTToRemove = new List<string>();

                for (int iR = 0; iR < numParts; iR++)
                {
                    int iStart = iR * partSize;
                    int iCount = (iR == numParts - 1 ? partSize + remainder : partSize);

                    lTasks.Add(Task.Factory.StartNew(() =>
                    {
                        List<string> sListRange = sMergedList[i].GetRange(iStart, iCount);
                        if (sListRange.Count > 0) { sListDTToMerge.Add(sListRange[0]); }

                        for (int i2 = 1; i2 < sListRange.Count; i2++)
                        {
                            DataTable dt1 = dsJson.Tables[sListRange[0]];
                            DataTable dt2 = dsJson.Tables[sListRange[i2]];
                            try
                            {
                                dt1.Merge(dt2);
                                lock (sListDTToRemove) { sListDTToRemove.Add(sListRange[i2]); }
                            }
                            catch (Exception ex) //on ne merge pas
                            {
                                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Merging '" + dt1.TableName + "' With '" + dt2.TableName + "' Failed : " + ex.Message); }
                                dt2.TableName = string.Concat(dt2.TableName, "_error");
                            }
                        }
                    }));
                }

                while (lTasks.Count(t => t.IsCompleted) < lTasks.Count)
                {
                    Thread.Sleep(100);
                }

                sListDTToRemove.Sort();
                //suppression des tables mergées
                foreach (string sTToRemove in sListDTToRemove)
                {
                    dsJson.Tables.Remove(sTToRemove);
                }

                //à ce stade, il reste autant de tables que de threads : celles-ci doivent être mergées en monothread
                sListDTToMerge.Sort();

                for (int iT = 1; iT < sListDTToMerge.Count; iT++)
                {
                    DataTable dt1 = dsJson.Tables[sListDTToMerge[0]];
                    DataTable dt2 = dsJson.Tables[sListDTToMerge[iT]];
                    try
                    {
                        dt1.Merge(dt2);
                        dsJson.Tables.Remove(dt2.TableName);
                    }
                    catch (Exception ex) //on ne merge pas
                    {
                        if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Merging '" + dt1.TableName + "' With '" + dt2.TableName + "' Failed : " + ex.Message); }
                        //dt2.TableName = string.Concat(dt2.TableName, "_error");
                    }
                }
            }

            //----------------------------------------------------------------------------------------------------------------------------------------------------ULTRA LONG

            //après le merge, je renomme les tables restantes en enlevant les index           
            for (int i = 0; i < dsJson.Tables.Count; i++)
            {
                string sDtName = Regex.Replace(dsJson.Tables[i].TableName, REGEX_JSON_TABLE, "$1");
                string sNamespace = string.Concat(dsJson.Tables[0].TableName, i > 0 ? ("_" + dsJson.Tables[i].TableName) : "");

                //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Renaming '" + dsJson.Tables[i].TableName + "' To '" + sDtName + "' (Namespace : " + sNamespace + ")"); }

                dsJson.Tables[i].TableName = sDtName;
                dsJson.Tables[i].Namespace = sNamespace;
            }

            if (AvoidSpecialCharsInColumnNames)
            {
                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Cleaning Up Special Characters in Column Names.."); }

                for (int iTt = 0; iTt < dsJson.Tables.Count; iTt++)
                {
                    dsJson.Tables[iTt].TableName = Toolbox.RemoveSpecialCharacters(dsJson.Tables[iTt].TableName, "_", true);
                    for (int iC = 0; iC < dsJson.Tables[iTt].Columns.Count; iC++)
                    {
                        dsJson.Tables[iTt].Columns[iC].ColumnName = Toolbox.RemoveSpecialCharacters(dsJson.Tables[iTt].Columns[iC].ColumnName, "_", true);
                    }
                }
            }

            if (ForcePrimaryKey.Length > 0)
            {

            }

            //retrait de la clé primaire
            if (RemovePrimaryKey)
            {
                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Removing Primary Key(s)"); }

                foreach (DataTable dt in dsJson.Tables)
                {
                    dt.PrimaryKey = null;
                }
            }
            Success = true;

            if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Json To DataSet Succesffully Finished !"); }
        }

        private static string CleanJsonValue(string sValue, bool bHandleNumerics)
        {
            char[] cValue = sValue.ToCharArray();
            for (int iC = 0; iC < cValue.Length; iC++)
            {
                if (cValue[iC].Equals(Convert.ToChar("["))) { cValue[iC] = Convert.ToChar("("); }
                if (cValue[iC].Equals(Convert.ToChar("{"))) { cValue[iC] = Convert.ToChar("("); }
                if (cValue[iC].Equals(Convert.ToChar("]"))) { cValue[iC] = Convert.ToChar(")"); }
                if (cValue[iC].Equals(Convert.ToChar("}"))) { cValue[iC] = Convert.ToChar(")"); }
            }
            sValue = new string(cValue);
            sValue = sValue.Replace("\"", "\"\"");
            sValue = Regex.Replace(sValue, "\\r", " ");
            sValue = Regex.Replace(sValue, "\\n", " ");

            if (sValue.Length == 0)
            {
                if (!bHandleNumerics) { sValue = string.Concat("\"", "", "\""); } else { sValue = "null"; }
            }
            else if (Toolbox.IsNumeric(sValue, CultureInf))
            {
                if (!bHandleNumerics)
                {
                    sValue = string.Concat("\"", sValue, "\"");
                }
                else
                {
                    if (!Toolbox.IsInteger(sValue, false, false))
                    {
                        sValue = string.Concat("\"", sValue, "\"");
                    }
                }
            }
            else if (Toolbox.IsBit1OrBoolean2(sValue) > 0)
            {
                if (!bHandleNumerics) { sValue = string.Concat("\"", sValue.ToLower(), "\""); } else { sValue = sValue.ToLower(); }
            }
            else
            {
                sValue = string.Concat("\"", sValue, "\"");
            }

            return sValue;
        }

        private DataSet CreateDataTableFromJsonPatterns(List<JsonPattern> jsP, int iIndexOffset, int iLevels, List<string> sParentNodes, DataSet dsLevelDown, CancellationToken cancellationToken)
        {
            bool bHasChildren = jsP.Last().Depth < iLevels;
            DataSet dsData = new DataSet(iIndexOffset.ToString());
            List<bool> bReferencesPrimaryKey = new List<bool>();
            List<int> iRow = new List<int>();
            int iElementInLevel = 0;

            try
            {
                for (int iR = 0; iR < jsP.Count; iR++)
                {
                    string sDtName = string.Concat(jsP[iR].NodeName.Length == 0 ? _sOutput : jsP[iR].NodeName, "{[", (iR + iIndexOffset).ToString() + "]}");
                    //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Creating Datatable : " + sDtName);
                    dsData.Tables.Add(new DataTable(sDtName));
                    iRow.Add(-1);
                    bReferencesPrimaryKey.Add(false);
                }

                var jsPP = jsP.Where(j => j.IsArray == false).ToList();

                for (int iR = 0; iR < jsP.Count; iR++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    string sLevelName = jsP[iR].NodeName.Length == 0 ? _sOutput : jsP[iR].NodeName;
                    string sParentName = sParentNodes[iR].Length == 0 ? _sOutput : sParentNodes[iR];

                    string sNode = jsP[iR].NodeName.Length == 0 ? _sOutput : jsP[iR].NodeName;
                    int iTable = dsData.Tables.IndexOf(string.Concat(jsP[iR].NodeName.Length == 0 ? _sOutput : jsP[iR].NodeName, "{[", (iR + iIndexOffset).ToString() + "]}"));
                    iRow[iTable]++;

                    if (!jsP[iR].IsArray)
                    {
                        iElementInLevel++;
                    }

                    string sDataDef = "";
                    //recherche du subniveau
                    char[] scJs = jsP[iR].Pattern.ToCharArray();
                    int iLevel = -1;
                    int iOpen = 0;
                    bool bClose = false;

                    //trouver la référence sur la table précédente
                    DataRow drDown = null;
                    if (!sParentName.Equals(sLevelName))
                    {
                        if (dsLevelDown.Tables.Contains(sParentName))
                        {
                            drDown = dsLevelDown.Tables[sParentName].Rows.Find(jsP[iR].ParentElement.ToString());
                        }
                        else
                        {
                            foreach (DataTable dtP in dsLevelDown.Tables)
                            {
                                if (dtP.Columns.Contains(jsP[iR].NodeName))
                                {
                                    drDown = dtP.Rows.Find(jsP[iR].ParentElement.ToString());
                                    if (drDown != null) { break; }
                                }
                            }
                        }
                    }

                    //if (drDown == null)
                    //{
                    //    Console.WriteLine("debug");
                    //}
                    int iIntoSentence = 0;

                    for (int iC = 0; iC < scJs.Length; iC++)
                    {
                        bClose = false;
                        if (scJs[iC].Equals(Convert.ToChar("{")) && iIntoSentence % 2 == 0) { iLevel++; if (iLevel == 1) { iOpen = iC; } }
                        else if (scJs[iC].Equals(Convert.ToChar("}")) && iIntoSentence % 2 == 0) { iLevel--; if (iLevel == 0) { bClose = true; } }
                        else if (scJs[iC].Equals(Convert.ToChar("[")) && iIntoSentence % 2 == 0) { iLevel++; if (iLevel == 1) { iOpen = iC; } }
                        else if (scJs[iC].Equals(Convert.ToChar("]")) && iIntoSentence % 2 == 0) { iLevel--; if (iLevel == 0) { bClose = true; } }
                        else if (scJs[iC].Equals(Convert.ToChar("\""))) //gestion des merdasses genre :    "name": "Re: [GLPI #0032083] Re: [GLPI #0032044] Urgent Impossibilité d'accès a citrix",
                        {
                            // "name": "Demande Réseau /   /  - Affectation accès à dossier public : Y:\\@_GROUPE_SERIS\\Distribution\\Amazon\\",
                            if (!scJs[iC - 1].Equals(Convert.ToChar("\\")) || (scJs[iC - 1].Equals(Convert.ToChar("\\")) && scJs[iC - 2].Equals(Convert.ToChar("\\"))))
                            {
                                iIntoSentence++;
                            }
                        }

                        if (iLevel == 0 && bClose)
                        {
                            for (int iS = iOpen; iS <= iC; iS++) //le "=" a beaucoup d'importance ! c'est le caractère qui referme le pattern
                            {
                                scJs[iS] = Convert.ToChar(" ");
                            }
                        }
                    }
                    sDataDef = new string(scJs);
                    if (sDataDef.IndexOf("\\/") > -1) { sDataDef = sDataDef.Replace("\\/", "/"); } //potentiellement couteux

                    MatchCollection mcKeyValue = Regex.Matches(sDataDef, @"(\s*""{1}.[^""\\]*""{1}\s*:{1}((""\s*"")|(\s*""([^\\""]|\\\\|\\""|\\t|\\s|\\n)*"")|(true)|(false)|(null)|(\s*""?[-.0-9]*""?)),?\s*)", RegexOptions.IgnoreCase);

                    string sLevelID = string.Concat(sLevelName, "_", "id");
                    string sParentID = Regex.IsMatch(sParentNodes[iR], REGEX_JSON_TABLE) ? string.Concat(Regex.Replace(sParentNodes[iR], REGEX_JSON_TABLE, "$1"), "_id") : string.Concat(_sOutput, "_id");
                    if (bHasChildren && !dsData.Tables[iTable].Columns.Contains(sLevelID))
                    {
                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding Primary Key Column : " + sLevelID);

                        dsData.Tables[iTable].Columns.Add(sLevelID);
                        dsData.Tables[iTable].PrimaryKey = new DataColumn[] { dsData.Tables[iTable].Columns[sLevelID] };
                    }
                    if (sParentNodes[iR].Length > 0 && !dsData.Tables[iTable].Columns.Contains(sParentID))
                    {
                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding Foreign Key Column : " + sParentID);

                        dsData.Tables[iTable].Columns.Add(sParentID);
                    }
                    if (ForcePrimaryKey.Length > 0 && drDown != null && drDown.Table.Columns.Contains(ForcePrimaryKey))
                    {
                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding Forced Foreign Key Column : " + ForcePrimaryKey);

                        dsData.Tables[iTable].Columns.Add(ForcePrimaryKey); bReferencesPrimaryKey[iTable] = true;
                    }

                    //cas N°1 : on est sur un clé/valeur classique
                    if (mcKeyValue.Count > 0)
                    {
                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Entering Key/Value Mode");

                        //colonnes
                        if (iRow[iTable] == 0)
                        {
                            foreach (Match mc in mcKeyValue)
                            {
                                string sCol = mc.Value; //.Trim();
                                char[] sSearchCol = sCol.ToCharArray();
                                int iGm = 0;
                                int iLastGuill = 0;
                                int iEnd = 0;
                                for (int iC = 0; iC < sSearchCol.Length; iC++)
                                {
                                    if (sSearchCol[iC].Equals(Convert.ToChar("\"")))
                                    {
                                        iGm++; iLastGuill = iC;
                                    }
                                    if (iGm > 1 && sSearchCol[iC].Equals(Convert.ToChar(":")))
                                    {
                                        iEnd = iC; break;
                                    }
                                }
                                //recherche d'un offset : ex : { "_id" : "5fa01aa4352e5e58fecd8b5a", "id_sample" 
                                //-> Il y a un espace avant le nom de colonne
                                int iOffset = 0;
                                for (int i = 0; i < sSearchCol.Length; i++)
                                {
                                    if (sSearchCol[i].Equals(Convert.ToChar(" "))
                                                  || sSearchCol[i].Equals(Convert.ToChar("\""))
                                                  || sSearchCol[i].Equals(Convert.ToChar("\r"))
                                                  || sSearchCol[i].Equals(Convert.ToChar("\t"))
                                                  || sSearchCol[i].Equals(Convert.ToChar("\n"))) { iOffset++; }
                                    else { break; }
                                }
                                sCol = new string(sSearchCol, iOffset, iEnd - (iEnd - iLastGuill) - iOffset);

                                //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding Column : " + sCol);

                                dsData.Tables[iTable].Columns.Add(sCol);
                            }
                        }

                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding " + mcKeyValue.Count.ToString() + " row(s)");

                        //valeurs
                        if (dsData.Tables[iTable].Columns.Count > 0)
                        {
                            DataRow dr = dsData.Tables[iTable].NewRow();
                            for (int iC = 0; iC < mcKeyValue.Count; iC++)
                            {
                                string sCol = mcKeyValue[iC].Value; //.Trim();
                                char[] sSearchCol = sCol.ToCharArray();
                                int iGm = 0;
                                int iLastGuill = 0;
                                int iEnd = 0;
                                for (int iCb = 0; iCb < sSearchCol.Length; iCb++)
                                {
                                    if (sSearchCol[iCb].Equals(Convert.ToChar("\"")))
                                    {
                                        iGm++; iLastGuill = iCb;
                                    }
                                    if (iGm > 1 && sSearchCol[iCb].Equals(Convert.ToChar(":")))
                                    {
                                        iEnd = iCb; break;
                                    }
                                }
                                sCol = new string(sSearchCol, 0, iEnd + 1);

                                string sValue = mcKeyValue[iC].Value[sCol.Length..];

                                //sValue = sValue.Replace("\"", "").Trim();

                                //------------------------------------------------AVRIL 2021 : NETTOYAGE
                                sValue = sValue.Replace("\\\"", "\"").Trim();
                                if (sValue.StartsWith("\"")) { sValue = sValue[1..]; }
                                if (sValue.EndsWith("\",")) { sValue = string.Concat(sValue[0..^2], ","); }
                                else if (sValue.EndsWith("\"")) { sValue = sValue[0..^1]; }
                                if (sValue.IndexOf("\\n") > -1) { sValue = sValue.Replace("\\n", Environment.NewLine); }
                                //------------------------------------------------AVRIL 2021

                                int iOffset = 0;
                                for (int i = 0; i < sSearchCol.Length; i++)
                                {
                                    if (sSearchCol[i].Equals(Convert.ToChar(" "))
                                            || sSearchCol[i].Equals(Convert.ToChar("\""))
                                            || sSearchCol[i].Equals(Convert.ToChar("\r"))
                                            || sSearchCol[i].Equals(Convert.ToChar("\t"))
                                            || sSearchCol[i].Equals(Convert.ToChar("\n"))) { iOffset++; }
                                    else { break; }
                                }
                                sCol = new string(sSearchCol, iOffset, iEnd - (iEnd - iLastGuill) - iOffset);

                                if (!dsData.Tables[iTable].Columns.Contains(sCol))
                                {
                                    dsData.Tables[iTable].Columns.Add(sCol);
                                }

                                if (sValue.EndsWith(",")) { sValue = sValue[0..^1]; }
                                if (sValue.Equals("null")) { sValue = string.Empty; }
                                try { dr[sCol] = sValue; }
                                catch (Exception ex)
                                {
                                    if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Unable to parse Value : '" + sValue + "' Into '" + sCol + "' (" + ex.Message + ")"); }
                                    throw;
                                }
                            }

                            if (bHasChildren)
                            {
                                try
                                {
                                    dr[sLevelID] = iElementInLevel.ToString();
                                }
                                catch (Exception ex)
                                {
                                    if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Unable to parse Value : '" + iElementInLevel.ToString() + "' Into '" + sLevelID + "' (" + ex.Message + ")"); }
                                    throw;
                                }
                            }
                            if (sParentNodes[iR].Length > 0)
                            {
                                try
                                {
                                    dr[sParentID] = jsP[iR].ParentElement.ToString();
                                }
                                catch (Exception ex)
                                {
                                    if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Unable to parse Value : '" + jsP[iR].ParentElement.ToString() + "' Into '" + sParentID + "' (" + ex.Message + ")"); }
                                    throw;
                                }
                            }

                            dsData.Tables[iTable].Rows.Add(dr);
                        }
                    }
                    else
                    {
                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Entering Array Mode");

                        //cas N°2 : on est sur un array
                        sDataDef = sDataDef.Replace(System.Environment.NewLine, " ");

                        if (sDataDef.StartsWith("[") && sDataDef.EndsWith("]"))
                        {
                            MatchCollection mcKeyValueArrayB = Regex.Matches(sDataDef, "\\[[\\s]*(\".[^\"]+\"){1}(,[\\s]*\".[^\"]+\")*[\\s]*\\]", RegexOptions.IgnoreCase);

                            if (ArraysAsNewTables)
                            {
                                if (mcKeyValueArrayB.Count > 0)
                                {
                                    if (!dsData.Tables[iTable].Columns.Contains(sNode))
                                    {
                                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding Column and Row : " + sNode);

                                        dsData.Tables[iTable].Columns.Add(sNode);
                                    }

                                    DataRow dr = dsData.Tables[iTable].NewRow();
                                    dr[sNode] = mcKeyValueArrayB.Count > 0 ? mcKeyValueArrayB[0].Value : sDataDef;

                                    if (sParentNodes[iR].Length > 0)
                                    {
                                        string sCID = string.Concat(sParentNodes[iR], "_id");
                                        //if (!dsData.Tables[iTable].Columns.Contains(sCID))
                                        //{ dsData.Tables[iTable].Columns.Add(sCID); }
                                        try
                                        {
                                            dr[sCID] = jsP[iR].ParentElement.ToString();
                                        }
                                        catch (Exception ex)
                                        {
                                            if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Unable to parse Value : '" + jsP[iR].ParentElement.ToString() + "' Into '" + sCID + "' (" + ex.Message + ")"); }
                                            throw;
                                        }
                                    }
                                    dsData.Tables[iTable].Rows.Add(dr);
                                }
                            }
                            else
                            {
                                if (drDown != null) { drDown[sNode] = sDataDef[1..^1]; }
                            }
                        }
                        else
                        {
                            //    string sCol = string.Concat(sNode, "_unknown_data");
                            //    if (!dsData.Tables[iTable].Columns.Contains(sCol))
                            //    {
                            //        dsData.Tables[iTable].Columns.Add(sCol);
                            //        dsData.Tables[iTable].Columns[sCol].Namespace = "UNKNOWN";
                            //    }
                            //    bHasUnknownData[iTable] = true;
                            //    dsData.Tables[iTable].Columns[sCol].SetOrdinal(dsData.Tables[iTable].Columns.Count - 1);
                            //    DataRow dr = dsData.Tables[iTable].NewRow();
                            //    dr[sCol] = sP;
                            //    dsData.Tables[iTable].Rows.Add(dr);
                        }

                    }
                    //si on veut forcer une clé primaire (retrouver la référence de l'élément inférieur)
                    if (drDown != null && ForcePrimaryKey.Length > 0 && bReferencesPrimaryKey[iTable])
                    {
                        //if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Adding Reference to Primary Key on Column : " + ForcePrimaryKey);

                        dsData.Tables[iTable].Rows[^1][ForcePrimaryKey] = drDown[ForcePrimaryKey];
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Processing = false;
                Success = false;
                throw;
            }
            catch (Exception ex)
            {
                if (OnJsonEvent != null) { OnJsonEvent(this, "\t * Unable to process Json Data (Element : " + iElementInLevel + ") : " + ex.Message); }
                DataTable dtError = new DataTable("JSONPARSER_EXCEPTION");
                dtError.Columns.Add("data");
                dtError.Columns.Add("exception");
                dtError.Columns.Add("source");
                DataRow dr = dtError.NewRow();
                dr[0] = _sJson;
                dr[1] = ex.Message;
                dr[2] = ex.Source ?? "";
                dtError.Rows.Add(dr);
                dsData.Tables.Add(dtError);
            }

            return dsData;
        }

        private List<JsonPattern> ParseJsonString(CancellationToken cancellationToken)
        {
            List<JsonPattern> jsP = new List<JsonPattern>();

            int iLevel = 0;
            List<int> iListPosParO = new List<int>();
            List<int> iElementInLevel = new List<int>();
            List<List<string>> sLevelNodeName = new List<List<string>>();

            bool bIsArray = false;
            int iIntoSentence = 0;
            string sName;

            char[] scJs = _sJson.ToCharArray();

            for (int iC = 0; iC < scJs.Length; iC++)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (iLevel >= iListPosParO.Count) { iListPosParO.Add(0); sLevelNodeName.Add(new List<string>()); }

                    bool bClose = false;

                    if (scJs[iC].Equals(Convert.ToChar("{")) && iIntoSentence % 2 == 0)
                    {
                        iListPosParO[iLevel] = iC;
                        iLevel++;
                        if (iElementInLevel.Count < iLevel) { iElementInLevel.Add(0); }
                        iElementInLevel[iLevel - 1]++;
                    }
                    else if (scJs[iC].Equals(Convert.ToChar("}")) && iIntoSentence % 2 == 0)
                    {
                        bClose = true;
                        iLevel--;
                        bIsArray = false;
                    }
                    else if (scJs[iC].Equals(Convert.ToChar("[")) && iIntoSentence % 2 == 0)
                    {
                        iListPosParO[iLevel] = iC;
                        bIsArray = true;
                    }
                    else if (scJs[iC].Equals(Convert.ToChar("]")) && iIntoSentence % 2 == 0)
                    {
                        bClose = true;
                    }
                    else if (scJs[iC].Equals(Convert.ToChar("\""))) //gestion des merdasses genre :    "name": "Re: [GLPI #0032083] Re: [GLPI #0032044] Urgent Impossibilité d'accès a citrix",
                    {
                        // "name": "Demande Réseau /   /  - Affectation accès à dossier public : Y:\\@_GROUPE_SERIS\\Distribution\\Amazon\\",
                        if (!scJs[iC - 1].Equals(Convert.ToChar("\\")) || (scJs[iC - 1].Equals(Convert.ToChar("\\")) && scJs[iC - 2].Equals(Convert.ToChar("\\"))))
                        {
                            iIntoSentence++;
                        }
                    }

                    if (bClose && iLevel >= 0)
                    {
                        string sPattern = new string(scJs, iListPosParO[iLevel], iC - iListPosParO[iLevel] + 1);

                        //if ((Regex.Matches(sPattern, "\\{").Count == Regex.Matches(sPattern, "\\}").Count) && (Regex.Matches(sPattern, "\\[").Count == Regex.Matches(sPattern, "\\]").Count))
                        //{
                        if ((sPattern.StartsWith("{") && sPattern.EndsWith("}")) || (sPattern.StartsWith("[") && sPattern.EndsWith("]")))
                        {
                            int iDepth = iLevel;

                            sName = JsonPattern.GetNameFromSubstring(scJs, iListPosParO[iLevel]);

                            //ajout janvier 2021 : pour corriger le problème de l'API v3 youtube : 
                            //si on ne trouve pas le nom du noeud, on prend le dernier nom de noeud existant dans ce niveau
                            if (sName.Length == 0 && sLevelNodeName[iLevel].Count > 0)
                            {
                                sName = sLevelNodeName[iLevel].Last();
                            }

                            //if (sName.Contains(":"))
                            //{ Console.WriteLine(""); }
                            sLevelNodeName[iLevel].Add(sName);

                            int iParentNode;
                            if (iLevel == 0) { iParentNode = 1; }
                            else
                            {
                                if (bIsArray) { iParentNode = (iLevel - 1 < 0) ? 1 : iElementInLevel[iLevel - 1]; }
                                else { iParentNode = iElementInLevel[iLevel - 1]; }
                            }

                            jsP.Add(new JsonPattern(sName.Length == 0 ? sLevelNodeName[iLevel][0] : sName, sPattern, bIsArray, iDepth, iParentNode));
                        }

                        if (bIsArray)
                        {
                            bIsArray = false;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Processing = false;
                    Success = false;
                    throw;
                }
            }
            return jsP;
        }

        private string DecodeEncodedNonAsciiCharacters(string value)
        {
            return Regex.Replace(
                value,
                @"\\u(?<Value>[a-zA-Z0-9]{4})",
                m =>
                {
                    return ((char)int.Parse(m.Groups["Value"].Value, NumberStyles.HexNumber)).ToString();
                });
        }

        #endregion

        private class JsonPattern
        {
            public string NodeName
            {
                get; set;
            }
            public string Pattern
            {
                get; set;
            }
            public bool IsArray
            {
                get;
            }
            public int Depth
            {
                get;
            }
            public int ParentElement
            {
                get;
            }

            public JsonPattern(string sName, string sPattern, bool bIsArray, int iDepth, int iParent)
            {
                Pattern = sPattern;
                IsArray = bIsArray;
                Depth = iDepth;
                ParentElement = iParent;
                NodeName = sName;
            }

            public static string GetNameFromSubstring(char[] sJson, int iPosStart)
            {
                List<int> iPosQuotes = new List<int>();
                int iParPos = 0;
                string sNodeName = "";

                int iOffset = 50;
                if (iPosStart - iOffset < 0) { iOffset = iPosStart; }

                //string s = "";
                //for (int i = iPosStart - iOffset; i < iPosStart; i++)
                //{ s = string.Concat(s, sJson[i]); }
                //string s = new string(sJson);
                //s = s.Substring(iPosStart - iOffset, iOffset);

                for (int i = iPosStart - iOffset; i < iPosStart; i++)
                {
                    if (sJson[i].Equals(Convert.ToChar("]")) || sJson[i].Equals(Convert.ToChar("}")))
                    {
                        iParPos = i;
                    }
                    if (sJson[i].Equals(Convert.ToChar("\"")))
                    {
                        iPosQuotes.Add(i);
                    }
                }
                if (iPosQuotes.Count >= 2)
                {
                    if (iPosQuotes.Last() > iParPos) // on est sur l'élément d'un niveau
                    {
                        for (int iQ = iPosQuotes[^2] + 1; iQ < iPosQuotes.Last(); iQ++)
                        {
                            sNodeName = string.Concat(sNodeName, sJson[iQ]);
                        }
                    }
                    //else //je sais pas mais ça fonctionne
                    //{
                    //    if (iPosLevelPrec > -1) 
                    //    { sNodeName = GetNameFromSubstring(sJson, iPosLevelPrec, -1); }
                    //}
                }

                return sNodeName;
            }
        }
    }

    internal static class SHSRegex
    {
        public const string REGEX_ISINTEGER = @"^[-]?[\d ]+[ ]*$"; //supporte nombre entier ou bien 1 5654 ou bien 4 564 , 00
        public const string REGEX_ISINTEGER_EXTENDED = @"^[-]?[\d ]+[ ]*[,.]{1}[ 0]+$";
        public const string REGEX_ISINTEGER_STARTSWITH_0 = @"^[0]{1}\d+$";
    }

    internal static class Toolbox
    {
        private const string REGEX_1x = "^-?[.,]?\\d+([.,]?\\d+)?$"; //" ^ ([-\\.),|[\\.,]).*";

        public static string RemoveSpecialCharacters(string str, string sReplacementChar, bool bSQL)
        {
            string sReturn = str;
            sReturn = RemoveDiacritics(sReturn);

            if (bSQL)
            {
                //Regex.Replace(dtC.ColumnName, @"[^\w\d]", "_")
                sReturn = Regex.Replace(sReturn, "[^a-zA-Z0-9]+", sReplacementChar, RegexOptions.Compiled);
            }

            return sReturn;
        }

        public static string RemoveDiacritics(string text)
        {
            string normalizedString = text.Normalize(NormalizationForm.FormD);
            var stringBuilder = new StringBuilder();

            foreach (char c in normalizedString)
            {
                UnicodeCategory unicodeCategory = CharUnicodeInfo.GetUnicodeCategory(c);
                if (unicodeCategory != UnicodeCategory.NonSpacingMark)
                {
                    stringBuilder.Append(c);
                }
            }

            return stringBuilder.ToString().Normalize(NormalizationForm.FormC);
        }

        public static bool IsNumeric(this string s, CultureInfo cultureInfo)
        {
            bool isNum;
            decimal decTest;

            //si il y a des données de type 01 ou 001, on garde le format varchar
            //note des nombres exotiques genre -.123 (-0,123)
            //-2,27373675443232E-13

            if (Regex.IsMatch(s, REGEX_1x))
            {
                //bool bOK = Decimal.TryParse(s.Replace(".", ","), NumberStyles.Any, CultureInfo.InvariantCulture, out _);
                try
                {
                    string sComma = ",";
                    string sDot = ".";
                    //format des décimaux
                    if (cultureInfo.NumberFormat.NumberDecimalSeparator.Equals('.'))
                    {
                        sComma = ".";
                        sDot = ",";
                    }

                    decTest = Convert.ToDecimal(s.Replace(sDot, sComma));
                    isNum = true;
                }
                catch { isNum = false; }
            }
            else { isNum = false; }

            return isNum;
        }

        public static bool IsInteger(this string s, bool bAllowSpecials, bool bAllow0First)
        {
            if (!bAllowSpecials && (s.IndexOf(",") > -1 || s.IndexOf(".") > -1))
            {
                // même si je reçois du 0.00 ou 3,00
                return false;
            }

            if (Regex.IsMatch(s, SHSRegex.REGEX_ISINTEGER, RegexOptions.Multiline))
            {
                if (Regex.IsMatch(s, SHSRegex.REGEX_ISINTEGER_STARTSWITH_0))
                {
                    if (bAllow0First) { return true; } else { return false; }
                }
                else { return true; }
            }
            else if (bAllowSpecials && Regex.IsMatch(s, SHSRegex.REGEX_ISINTEGER_EXTENDED, RegexOptions.Multiline))
            {
                return true;
            }
            else { return false; }
        }

        public static int IsBit1OrBoolean2(string sValue)
        {
            sValue = sValue.Trim();
            int iOK = 0;

            if (sValue.Length <= 5) // on se fait pas chier à analyser des champs trop longs, ce ne sera pas du format boolean
            {
                sValue = sValue.ToLower();
                if (sValue.Equals("0") | sValue.Equals("1"))
                {
                    iOK = 1;
                }
                else if (sValue.Equals("true") | sValue.Equals("false") | sValue.Equals("oui") | sValue.Equals("non"))
                {
                    iOK = 2;
                }
            }

            return iOK;

        }

    }
}
