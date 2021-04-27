namespace PluginODBC.DataContracts
{
    public class ConfigureReplicationFormData
    {
        public string SchemaName { get; set; }
        public string GoldenTableName { get; set; }
        public string VersionTableName { get; set; }
    }
}