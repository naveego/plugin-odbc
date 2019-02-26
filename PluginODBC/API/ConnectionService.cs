using System.Data.Odbc;
using PluginODBC.Interfaces;

namespace PluginODBC.API
{
    public class ConnectionService : IConnectionService
    {
        public OdbcConnection Connection { get; }

        public ConnectionService(OdbcConnection connection)
        {
            Connection = connection;
        }

        public void Open()
        {
            Connection.Open();
        }
        
        public void Close()
        {
            Connection.Close();
        }
    }
}