using System.Collections.Generic;

namespace PluginODBC.API.Read
{
    public class RealTimeSettings
    {
        public int PollingIntervalSeconds { get; set; } = 5;
        public List<JournalInfo> TableInformation {get; set; } = new List<JournalInfo>();

        public class JournalInfo {
            public string TargetJournalLibrary {get; set;} = "";
            public string TargetJournalName {get; set;} = "";
            public string TargetTableLibrary {get; set;} = "";
            public string TargetTableName {get; set;} = "";
            public string TargetTableAlias { get; set; } = "";

            public string GetTargetJournalAlias()
            {
                return $"{TargetJournalLibrary}_{TargetJournalName}";
            }
            
            public string GetTargetTableName()
            {
                return $"{TargetTableLibrary}_{TargetTableName}";
            }
            
            public string GetTargetTableAlias()
            {
                if (!string.IsNullOrWhiteSpace(TargetTableAlias))
                {
                    return TargetTableAlias;
                }
                
                return $"{TargetTableLibrary}.{TargetTableName}";
            }
        };
    }
}