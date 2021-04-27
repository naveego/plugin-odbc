namespace PluginODBC.DataContracts
{
    public class ReplicationColumn
    {
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public bool PrimaryKey { get; set; }
        public bool Serialize = false;
    }
}