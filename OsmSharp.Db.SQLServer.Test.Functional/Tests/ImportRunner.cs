// OsmSharp - OpenStreetMap (OSM) SDK
// Copyright (C) 2016 Abelshausen Ben
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

using OsmSharp.Data.SQLServer.Test.Functional.Properties;
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

                Osm.SchemaTools.SchemaTools.SnapshotDbCreateAndDetect(connection);
                Osm.SchemaTools.SchemaTools.SnapshotDbDeleteAllData(connection);
                using (var stream = File.OpenRead("belgium-latest.osm.pbf"))
                {
                    var source = new OsmSharp.Osm.PBF.Streams.PBFOsmStreamSource(stream);
                    var progress = new OsmSharp.Osm.Streams.Filters.OsmStreamFilterProgress();
                    progress.RegisterSource(source);
                    var target = new OsmSharp.Data.SQLServer.Osm.Streams.SnapshotDbStreamTarget(
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

                Osm.SchemaTools.SchemaTools.HistoryDbCreateAndDetect(connection);
                Osm.SchemaTools.SchemaTools.HistoryDbDeleteAllData(connection);
                using (var stream = File.OpenRead("belgium-latest.osm.pbf"))
                {
                    var source = new OsmSharp.Osm.PBF.Streams.PBFOsmStreamSource(stream);
                    var progress = new OsmSharp.Osm.Streams.Filters.OsmStreamFilterProgress();
                    progress.RegisterSource(source);
                    var target = new OsmSharp.Data.SQLServer.Osm.Streams.HistoryDbStreamTarget(
                        Settings.Default.ConnectionString, true);
                    target.RegisterSource(progress);
                    target.Pull();
                }
            }
        }
    }
}
