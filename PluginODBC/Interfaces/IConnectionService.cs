using System.Data.Odbc;

namespace PluginODBC.Interfaces
{
    public interface IConnectionService
    {
        void Open();
        void Close();

        OdbcConnection Connection { get; }
    }
}