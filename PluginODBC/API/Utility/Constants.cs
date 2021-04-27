using System.Collections.Generic;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Utility
{
    public static class Constants
    {
        public static string ReplicationRecordId = "NaveegoReplicationRecordId";
        public static string ReplicationVersionIds = "NaveegoVersionIds";
        public static string ReplicationVersionRecordId = "NaveegoReplicationVersionRecordId";
        
        public static string ReplicationMetaDataTableName = "NaveegoReplicationMetaData";
        public static string ReplicationMetaDataJobId = "NaveegoJobId";
        public static string ReplicationMetaDataRequest = "Request";
        public static string ReplicationMetaDataReplicatedShapeId = "NaveegoShapeId";
        public static string ReplicationMetaDataReplicatedShapeName = "NaveegoShapeName";
        public static string ReplicationMetaDataTimestamp = "Timestamp";

        public static List<ReplicationColumn> ReplicationMetaDataColumns = new List<ReplicationColumn>
        {
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataJobId,
                DataType = "varchar(255)",
                PrimaryKey = true
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataRequest,
                PrimaryKey = false,
                DataType = "clob"
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataReplicatedShapeId,
                DataType = "varchar(255)",
                PrimaryKey = false
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataReplicatedShapeName,
                DataType = "clob",
                PrimaryKey = false
            },
            new ReplicationColumn
            {
                ColumnName = ReplicationMetaDataTimestamp,
                DataType = "varchar(255)",
                PrimaryKey = false
            }
        };
    }
}