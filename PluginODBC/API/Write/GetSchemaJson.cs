using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Write
{
    public static partial class Write
    {
        public static string GetSchemaJson(List<WriteStoredProcedure> storedProcedures)
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {
                    "properties", new Dictionary<string, object>
                    {
                        {
                            "Query", new Dictionary<string, string>
                            {
                                {"type", "string"},
                                {"title", "Query"},
                                {"description", "Query to execute for write back with parameter place holders"},
                            }
                        },
                        {
                            "Parameters", new Dictionary<string, object>
                            {
                                {"type", "array"},
                                {"title", "Parameters"},
                                {"description", "Parameters to replace the place holders in the query"},
                                {
                                    "items", new Dictionary<string, object>
                                    {
                                        {"type", "object"},
                                        {
                                            "properties", new Dictionary<string, object>
                                            {
                                                {
                                                    "ParamName", new Dictionary<string, object>
                                                    {
                                                        {"type", "string"},
                                                        {"title", "Name"}
                                                    }
                                                },
                                                {
                                                    "ParamType", new Dictionary<string, object>
                                                    {
                                                        {"type", "string"},
                                                        {"title", "Type"},
                                                        {
                                                            "enum", new[]
                                                            {
                                                                "string", "bool", "int", "float", "decimal"
                                                            }
                                                        },
                                                        {
                                                            "enumNames", new[]
                                                            {
                                                                "String", "Bool", "Int", "Float", "Decimal"
                                                            }
                                                        },
                                                    }
                                                },
                                            }
                                        },
                                        {
                                            "required", new[]
                                            {
                                                "ParamName", "ParamType"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                    }
                },
                {
                    "required", new[]
                    {
                        "Query"
                    }
                }
            };

            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}