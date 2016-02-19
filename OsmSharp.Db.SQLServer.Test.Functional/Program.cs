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

using OsmSharp.Data.SQLServer.Osm.SchemaTools;
using OsmSharp.Data.SQLServer.Test.Functional.Properties;
using System;
using System.Data.SqlClient;

namespace OsmSharp.Data.SQLServer.Test.Functional
{
    class Program
    {
        static void Main(string[] args)
        {
            // enable logging.
            OsmSharp.Logging.Log.Enable();
            OsmSharp.Logging.Log.RegisterListener(new ConsoleTraceListener());

            // download all data.
            Staging.Download.DownloadAll();

            // try importing into db.
            using (var connection = new System.Data.SqlClient.SqlConnection(
                Settings.Default.ConnectionString))
            {
                connection.Open();
                SchemaTools.HistoryDbDropSchema(connection);
                SchemaTools.SnapshotDbDropSchema(connection);
            }
            //Tests.ImportRunner.TestImportSnapshotDbBelgium();
            Tests.ImportRunner.TestImportHistoryDbBelgium();

            var source = new OsmSharp.Data.SQLServer.Osm.Streams.HistoryDbStreamSource(
                Settings.Default.ConnectionString);
            var progress = new OsmSharp.Osm.Streams.Filters.OsmStreamFilterProgress();
            progress.RegisterSource(source);
            foreach (var data in progress)
            {
                //Console.WriteLine(data);
            }
        }
    }
}
