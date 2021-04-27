using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginODBC.API.Read
{
    public static partial class Read
    {
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {
                    "ui:order", new[]
                    {
                        "PollingInterval",
                        "TableInformation"
                    }
                },
                {
                    "TableInformation", new Dictionary<string,object>
                    {
                        {
                            "ui:order", new[]
                            {
                                "TargetJournalLibrary",
                                "TargetJournalName",
                                "TargetTableLibrary",
                                "TargetTableName",
                                "TargetTableAlias"
                            }
                        }
                    }
                }
            };

            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}