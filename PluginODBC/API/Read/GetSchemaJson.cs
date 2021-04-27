using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginODBC.API.Read
{
    public static partial class Read
    {
        public static string GetSchemaJson()
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    {"PollingInterval", new Dictionary<string, object>
                    {
                        {"type", "number"},
                        {"title", "Polling Interval"},
                        {"description", "How frequently to poll the api for changes in seconds (default 5s)."},
                        {"default", 5},
                    }},
                    {"TableInformation", new Dictionary<string, object>
                    {
                        {"type", "array"},
                        {"title", "Table Information"},
                        {"description", "Information about the tables to monitor for changes."},
                        {"items", new Dictionary<string, object>
                        {
                            {"type", "object"},
                            {"properties", new Dictionary<string, object>
                            {
                                {"TargetJournalLibrary", new Dictionary<string, string>
                                {
                                    {"type", "string"},
                                    {"title", "Target Journal Library"},
                                    {"description", "The name of the library the target journal file is in."},
                                }},
                                {"TargetJournalName", new Dictionary<string, string>
                                {
                                    {"type", "string"},
                                    {"title", "Target Journal Name"},
                                    {"description", "The name of the Journal file to query for changes."},
                                }},
                                {"TargetTableLibrary", new Dictionary<string, string>
                                {
                                    {"type", "string"},
                                    {"title", "Target Table Library"},
                                    {"description", "The name of the library the target table file is in."},
                                }},
                                {"TargetTableName", new Dictionary<string, string>
                                {
                                    {"type", "string"},
                                    {"title", "Target Table Name"},
                                    {"description", "The name of the table file being monitored for changes."},
                                }},
                                {"TargetTableAlias", new Dictionary<string, string>
                                {
                                    {"type", "string"},
                                    {"title", "Target Table Alias"},
                                    {"description", "The alias of the table file as defined in the query being monitored for changes."},
                                }},
                            }},
                            {"required", new []
                            {
                                "TargetJournalLibrary",
                                "TargetJournalName",
                                "TargetTableLibrary",
                                "TargetTableName"
                            }}
                        }}
                    }},
                }},
                {"required", new []
                {
                    "PollingInterval",
                    "TableInformation"
                }}
            };
            
            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}