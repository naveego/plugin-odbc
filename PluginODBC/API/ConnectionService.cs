using System.Data;
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

        public DataTable GetSchema()
        {
            return Connection.GetSchema();
        }
    }
}