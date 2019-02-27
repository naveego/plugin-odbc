using System.Data.Odbc;

namespace PluginODBC.Interfaces
{
    public interface ICommandService
    {
        IReaderService ExecuteReader();
        OdbcParameter AddParameter(string parameterName, OdbcType odbcType);
        void Prepare();
    }
}