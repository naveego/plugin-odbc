using System.Data.Odbc;

namespace PluginODBC.Interfaces
{
    public interface IConnectionService
    {
        OdbcConnection MakeDbObject();
    }
}