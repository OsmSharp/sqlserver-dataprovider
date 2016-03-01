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
using OsmSharp.Db.SQLServer.Streams;
using OsmSharp.Streams.Filters;
using System;

namespace OsmSharp.Data.SQLServer.Test.Functional
{
    class Program
    {
        static void Main(string[] args)
        {
            // enable logging.
            OsmSharp.Logging.Logger.LogAction = (origin, level, message, parameters) =>
            {
                Console.WriteLine("{0}:{1} - {2}", origin, level, message);
            };

            // download all data.
            Staging.Download.DownloadAll();

            // try importing into db.
            using (var connection = new System.Data.SqlClient.SqlConnection(
                Settings.Default.ConnectionString))
            {
                connection.Open();
                Db.SQLServer.Schema.Tools.HistoryDbDropSchema(connection);
                Db.SQLServer.Schema.Tools.SnapshotDbDropSchema(connection);
            }

            //Tests.ImportRunner.TestImportSnapshotDbBelgium();
            Tests.ImportRunner.TestImportHistoryDbBelgium();

            var source = new HistoryDbStreamSource(
                Settings.Default.ConnectionString);
            var progress = new OsmStreamFilterProgress();
            progress.RegisterSource(source);
            foreach (var data in progress)
            {
                //Console.WriteLine(data);
            }
        }
    }
}
