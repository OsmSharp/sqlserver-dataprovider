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

using OsmSharp.Data.SQLServer.Test.Functional.Properties;
using OsmSharp.Db.SQLServer.Schema;
using OsmSharp.Db.SQLServer.Streams;
using OsmSharp.Streams;
using OsmSharp.Streams.Filters;
using System.IO;

namespace OsmSharp.Data.SQLServer.Test.Functional.Tests
{
    /// <summary>
    /// An import runner.
    /// </summary>
    public static class ImportRunner
    {
        /// <summary>
        /// Tests importing belgium into the db.
        /// </summary>
        public static void TestImportSnapshotDbBelgium()
        {
            using (var connection = new System.Data.SqlClient.SqlConnection(
                Settings.Default.ConnectionString))
            {
                connection.Open();

                Tools.SnapshotDbCreateAndDetect(connection);
                Tools.SnapshotDbDeleteAll(connection);
                using (var stream = File.OpenRead("belgium-latest.osm.pbf"))
                {
                    var source = new PBFOsmStreamSource(stream);
                    var progress = new OsmStreamFilterProgress();
                    progress.RegisterSource(source);
                    var target = new SnapshotDbStreamTarget(
                        Settings.Default.ConnectionString, true);
                    target.RegisterSource(progress);
                    target.Pull();
                }
            }
        }

        /// <summary>
        /// Tests importing belgium into the db.
        /// </summary>
        public static void TestImportHistoryDbBelgium()
        {
            using (var connection = new System.Data.SqlClient.SqlConnection(
                Settings.Default.ConnectionString))
            {
                connection.Open();

                Tools.HistoryDbCreateAndDetect(connection);
                Tools.HistoryDbDeleteAll(connection);
                using (var stream = File.OpenRead("belgium-latest.osm.pbf"))
                {
                    var source = new PBFOsmStreamSource(stream);
                    var progress = new OsmStreamFilterProgress();
                    progress.RegisterSource(source);
                    var target = new HistoryDbStreamTarget(
                        Settings.Default.ConnectionString, true);
                    target.RegisterSource(progress);
                    target.Pull();
                }
            }
        }
    }
}
