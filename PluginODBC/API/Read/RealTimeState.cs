using System.Collections.Generic;

namespace PluginODBC.API.Read
{
    public class RealTimeState
    {
        public Dictionary<string,long> LastJournalEntryIdMap { get; set; } = new Dictionary<string, long>();
        public long JobVersion { get; set; } = -1;
        public long ShapeVersion { get; set; } = -1;
    }
}