using System.Data;
using System.Data.Odbc;
using PluginODBC.Interfaces;

namespace PluginODBC.API
{
    public class ReaderService : IReaderService
    {
        private readonly OdbcDataReader _reader;

        public bool HasRows => _reader.HasRows;
        public int RecordsAffected => _reader.RecordsAffected;
        public object this[string index] => _reader[index];

        public ReaderService(OdbcDataReader reader)
        {
            _reader = reader;
        }

        public bool Read()
        {
            return _reader.Read();
        }

        public void Close()
        {
            _reader.Close();
        }

        public DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }
    }
}