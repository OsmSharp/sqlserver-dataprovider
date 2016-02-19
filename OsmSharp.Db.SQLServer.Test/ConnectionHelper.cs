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

using OsmSharp.Db.SQLServer.Test.Properties;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer.Test
{
    /// <summary>
    /// A connection helper.
    /// </summary>
    public static class ConnectionHelper
    {
        ///// <summary>
        ///// Gets a connection to an empty history test db.
        ///// </summary>
        //public static SqlConnection GetConnectionToEmptyTestHistoryDb()
        //{
        //    var connection = new System.Data.SqlClient.SqlConnection(
        //        Settings.Default.ConnectionString);
        //    connection.Open();
        //    OsmSharp.Db.SQLServer.Schema.Tools.HistoryDbDropSchema(connection);
        //    OsmSharp.Db.SQLServer.Schema.Tools.HistoryDbCreateAndDetect(connection);
        //    return connection;
        //}

        /// <summary>
        /// Gets a connection to an empty snapshot test db.
        /// </summary>
        public static SqlConnection GetConnectionToEmptyTestSnapshotDb()
        {
            var connection = new System.Data.SqlClient.SqlConnection(
                Settings.Default.ConnectionString);
            connection.Open();
            OsmSharp.Db.SQLServer.Schema.Tools.SnapshotDbDropSchema(connection);
            OsmSharp.Db.SQLServer.Schema.Tools.SnapshotDbCreateAndDetect(connection);
            return connection;
        }
    }
}