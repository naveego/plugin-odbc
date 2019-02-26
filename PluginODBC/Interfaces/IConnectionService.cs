using System.Data;
using System.Data.Odbc;

namespace PluginODBC.Interfaces
{
    public interface IConnectionService
    {
        void Open();
        void Close();
        DataTable GetSchema();
        OdbcConnection Connection { get; }
    }
}