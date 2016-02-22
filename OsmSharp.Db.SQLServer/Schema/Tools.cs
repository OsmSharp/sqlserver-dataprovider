// The MIT License (MIT)

// Copyright (c) 2016 Ben Abelshausen

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;

namespace OsmSharp.Db.SQLServer.Schema
{
    /// <summary>
    /// Static schema tools.
    /// </summary>
    public static class Tools
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

            OsmSharp.Logging.Logger.Log("OsmSharp.Db.SQLServer.SchemaTools.SQLServerSchemaTools", 
                OsmSharp.Logging.TraceEventType.Information,
                    "Creating snapshot database schema...");
            ExecuteSQL(connection, "SnapshotDbSchemaDDL.sql");
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

            OsmSharp.Logging.Logger.Log("SQLServerSchemaTools",
                OsmSharp.Logging.TraceEventType.Information,
                    "Creating history database schema...");
            ExecuteSQL(connection, "HistoryDbSchemaDDL.sql");
        }

        /// <summary>
        /// Drops the snapshot schema.
        /// </summary>
        public static void SnapshotDbDropSchema(SqlConnection connection)
        {
            OsmSharp.Logging.Logger.Log("SQLServerSchemaTools", 
                OsmSharp.Logging.TraceEventType.Information, 
                    "Dropping snapshot database schema...");
            ExecuteSQL(connection, "SnapshotDbSchemaDROP.sql");
        }

        /// <summary>
        /// Drops the history schema.
        /// </summary>
        public static void HistoryDbDropSchema(SqlConnection connection)
        {
            OsmSharp.Logging.Logger.Log("SQLServerSchemaTools",
                OsmSharp.Logging.TraceEventType.Information,
                    "Dropping history database schema...");
            ExecuteSQL(connection, "HistoryDbSchemaDROP.sql");
        }

        /// <summary>
        /// Adds indexes, removes duplicates and adds constraints
        /// </summary>
        public static void SnapshotDbAddConstraints(SqlConnection connection)
        {
            OsmSharp.Logging.Logger.Log("SQLServerSchemaTools", 
                OsmSharp.Logging.TraceEventType.Information, 
                    "Adding snapshot database constraints...");
            ExecuteSQL(connection, "SnapshotDbSchemaConstraints.sql");
        }

        /// <summary>
        /// Deletes all data.
        /// </summary>
        public static void SnapshotDbDeleteAll(SqlConnection connection)
        {
            OsmSharp.Logging.Logger.Log("SQLServerSchemaTools",
                OsmSharp.Logging.TraceEventType.Information,
                    "Deleting snapshot all data...");
            ExecuteSQL(connection, "SnapshotDbDeleteAllData.sql");
        }

        /// <summary>
        /// Deletes all data.
        /// </summary>
        public static void HistoryDbDeleteAll(SqlConnection connection)
        {
            OsmSharp.Logging.Logger.Log("SQLServerSchemaTools",
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