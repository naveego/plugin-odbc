using System.Data.Odbc;
using PluginODBC.Helper;
using PluginODBC.Interfaces;

namespace PluginODBC.API
{
    public class ConnectionFactoryService : IConnectionFactoryService
    {
        private readonly Settings _settings;
        
        public ConnectionFactoryService(Settings settings)
        {
            _settings = settings;
        }
        
        public IConnectionService MakeConnectionObject()
        {
            var connString = _settings.GetConnectionString();
            return new ConnectionService(new OdbcConnection(connString));
        }

        public ICommandService MakeCommandObject(string query, IConnectionService connection)
        {
            return new CommandService(new OdbcCommand(query, connection.Connection) {CommandTimeout = _settings.QueryTimeout});
        }
    }
}