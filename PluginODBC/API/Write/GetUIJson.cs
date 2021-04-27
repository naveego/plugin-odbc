using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginODBC.API.Write
{
    public static partial class Write
    {
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {
                    "ui:order", new[]
                    {
                        "Query", "Parameters"
                    }
                },
                {
                    "Query", new Dictionary<string, object>
                    {
                        {"ui:widget", "textarea"}
                    }
                }
            };
            
            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}