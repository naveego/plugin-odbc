using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginODBC.API.Replication
{
    public static partial class Replication
    {
        public static string GetSchemaJson()
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    {"SchemaName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Schema Name"},
                        {"description", "Name of schema to put golden and version tables into in ODBC"},
                    }},
                    {"GoldenTableName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Golden Record Table Name"},
                        {"description", "Name for your golden record table in ODBC"},
                    }},
                    {"VersionTableName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Version Record Bucket Name"},
                        {"description", "Name for your version record table in ODBC"},
                    }},
                }},
                {"required", new []
                {
                    "SchemaName",
                    "GoldenTableName",
                    "VersionTableName"
                }}
            };
            
            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}