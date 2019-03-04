using System.Data.Odbc;

namespace PluginODBC.Interfaces
{
    public interface IConnectionFactoryService
    {
        IConnectionService MakeConnectionObject();
        ICommandService MakeCommandObject(string query, IConnectionService connection);
    }
}