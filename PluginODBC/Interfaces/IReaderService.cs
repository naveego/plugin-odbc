using System.Data;

namespace PluginODBC.Interfaces
{
    public interface IReaderService
    {
        DataTable GetSchemaTable();
        bool Read();
        void Close();
        int RecordsAffected { get; }
        bool HasRows { get; }
        int FieldCount { get; }
        object this[string value] { get; }
    }
}