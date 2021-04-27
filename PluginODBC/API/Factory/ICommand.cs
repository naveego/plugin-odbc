using System.Data.Odbc;
using System.Threading.Tasks;

namespace PluginODBC.API.Factory
{
    public interface ICommand
    {
        void SetConnection(IConnection conn);
        void SetCommandText(string commandText);
        void AddParameter(string name, object value);
        void AddParameter(string parameterName, object value, OdbcType odbcType);
        Task<IReader> ExecuteReaderAsync();
        Task<int> ExecuteNonQueryAsync();
        void Prepare();
    }
}