using System.Data.Odbc;
using PluginODBC.Helper;
using PluginODBC.Interfaces;

namespace PluginODBC.API
{
    public class ConnectionService : IConnectionService
    {
        private readonly Settings _settings;
        
        public ConnectionService(Settings settings)
        {
            _settings = settings;
        }
        
        public OdbcConnection MakeDbObject()
        {
            var connString = _settings.GetConnectionString();
            return new OdbcConnection(connString);
        }
    }
}