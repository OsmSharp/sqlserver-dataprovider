// OsmSharp - OpenStreetMap (OSM) SDK
//
// Copyright (C) 2016 Abelshausen Ben
//                    Alexander Sinitsyn
// 
// This file is part of OsmSharp.
// 
// OsmSharp is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 2 of the License, or
// (at your option) any later version.
// 
// OsmSharp is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with OsmSharp. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;

namespace OsmSharp.Data.SQLServer.Osm.SchemaTools
{
    /// <summary>
    /// Static schema tools.
    /// </summary>
    public static class SchemaTools
    {
        /// <summary>
        /// Creates/detects the snapshot db schema.
        /// </summary>
        public static void SnapshotDbCreateAndDetect(SqlConnection connection)
        {
            //check if Simple Schema table exists
            const string sql = "select object_id('dbo.node', 'U')";
            object res;
            using (var cmd = new SqlCommand(sql, connection))
            {
                res = cmd.ExecuteScalar();
            }

            //if table exists, we are OK
            if (!DBNull.Value.Equals(res))
                return;

            OsmSharp.Logging.Log.TraceEvent("OsmSharp.Data.SQLServer.Osm.SchemaTools.SQLServerSchemaTools", 
                OsmSharp.Logging.TraceEventType.Information,
                    "Creating snapshot database schema...");
            ExecuteSQL(connection, "SnapshopDbSchemaDDL.sql");
        }
        /// <summary>
        /// Creates/detects the history db schema.
        /// </summary>
        public static void HistoryDbCreateAndDetect(SqlConnection connection)
        {
            //check if Simple Schema table exists
            const string sql = "select object_id('dbo.node', 'U')";
            object res;
            using (var cmd = new SqlCommand(sql, connection))
            {
                res = cmd.ExecuteScalar();
            }

            //if table exists, we are OK
            if (!DBNull.Value.Equals(res))
                return;

            OsmSharp.Logging.Log.TraceEvent("SQLServerSchemaTools",
                OsmSharp.Logging.TraceEventType.Information,
                    "Creating history database schema...");
            ExecuteSQL(connection, "HistoryDbSchemaDDL.sql");
        }

        /// <summary>
        /// Drops the snapshot schema.
        /// </summary>
        public static void SnapshotDbDropSchema(SqlConnection connection)
        {
            OsmSharp.Logging.Log.TraceEvent("SQLServerSchemaTools", 
                OsmSharp.Logging.TraceEventType.Information, 
                    "Dropping snapshot database schema...");
            ExecuteSQL(connection, "SnapshotDbSchemaDROP.sql");
        }

        /// <summary>
        /// Drops the history schema.
        /// </summary>
        public static void HistoryDbDropSchema(SqlConnection connection)
        {
            OsmSharp.Logging.Log.TraceEvent("SQLServerSchemaTools",
                OsmSharp.Logging.TraceEventType.Information,
                    "Dropping history database schema...");
            ExecuteSQL(connection, "HistoryDbSchemaDROP.sql");
        }

        /// <summary>
        /// Adds indexes, removes duplicates and adds constraints
        /// </summary>
        public static void SnapshotDbAddConstraints(SqlConnection connection)
        {
            OsmSharp.Logging.Log.TraceEvent("SQLServerSchemaTools", 
                OsmSharp.Logging.TraceEventType.Information, 
                    "Adding snapshot database constraints...");
            ExecuteSQL(connection, "SnapshotDbSchemaConstraints.sql");
        }

        /// <summary>
        /// Deletes all data.
        /// </summary>
        public static void SnapshotDbDeleteAllData(SqlConnection connection)
        {
            OsmSharp.Logging.Log.TraceEvent("SQLServerSchemaTools",
                OsmSharp.Logging.TraceEventType.Information,
                    "Deleting snapshot all data...");
            ExecuteSQL(connection, "SnapshotDbDeleteAllData.sql");
        }

        /// <summary>
        /// Deletes all data.
        /// </summary>
        public static void HistoryDbDeleteAllData(SqlConnection connection)
        {
            OsmSharp.Logging.Log.TraceEvent("SQLServerSchemaTools",
                OsmSharp.Logging.TraceEventType.Information,
                    "Deleting history all data...");
            ExecuteSQL(connection, "HistoryDbDeleteAllData.sql");
        }

        /// <summary>
        /// Executes the sql in the given resource file.
        /// </summary>
        private static void ExecuteSQL(SqlConnection connection, string resourceFilename)
        {
            foreach (string resource in Assembly.GetExecutingAssembly().GetManifestResourceNames().Where(resource => resource.EndsWith(resourceFilename)))
            {
                using (Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resource))
                {
                    if (stream != null)
                    {
                        using (var reader = new StreamReader(stream))
                        {
                            using (var cmd = new SqlCommand("", connection))
                            {
                                cmd.CommandTimeout = 1800;  // 30 minutes. Adding constraints can be time consuming
                                cmd.CommandText = reader.ReadToEnd();
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                }

                break;
            }
        }
    }
}