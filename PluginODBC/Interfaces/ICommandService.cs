using System.Data.Odbc;

namespace PluginODBC.Interfaces
{
    public interface ICommandService
    {
        IReaderService ExecuteReader();
        OdbcParameterCollection Parameters { get; }
    }
}