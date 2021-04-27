using System.Data.Odbc;
using System.Threading.Tasks;

namespace PluginODBC.API.Factory
{
    public class Command : ICommand
    {
        private readonly OdbcCommand _cmd;

        public Command()
        {
            _cmd = new OdbcCommand();
        }

        public Command(string commandText)
        {
            _cmd = new OdbcCommand(commandText);
        }

        public Command(string commandText, IConnection conn)
        {
            _cmd = new OdbcCommand(commandText, (OdbcConnection) conn.GetConnection());
        }

        public void SetConnection(IConnection conn)
        {
            _cmd.Connection = (OdbcConnection) conn.GetConnection();
        }

        public void SetCommandText(string commandText)
        {
            _cmd.CommandText = commandText;
        }

        public void AddParameter(string name, object value)
        {
            _cmd.Parameters.Add(name, value);
        }
        
        public void AddParameter(string parameterName, object value, OdbcType odbcType)
        {
            var param = _cmd.Parameters.Add(parameterName, odbcType);
            param.Value = value;
        }

        public async Task<IReader> ExecuteReaderAsync()
        {
            return new Reader(await _cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            return await _cmd.ExecuteNonQueryAsync();
        }
        
        public void Prepare()
        {
            _cmd.Prepare();
        }
    }
}