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

using OsmSharp.Streams;
using NUnit.Framework;
using OsmSharp.Tags;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer.Test.Streams
{
    /// <summary>
    /// Contains tests for the history db stream target.
    /// </summary>
    [TestFixture]
    public class HistoryDbStreamTargetTests
    {
        /// <summary>
        /// Test loading one node.
        /// </summary>
        [Test]
        public void TestWriteNode()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestHistoryDb();
            var target = new OsmSharp.Db.SQLServer.Streams.HistoryDbStreamTarget(connection);

            target.RegisterSource(new OsmGeo[]
            {
                new Node()
                {
                    Id = 1,
                    Latitude = 2,
                    Longitude = 3,
                    Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                    ChangeSetId = 12,
                    TimeStamp = new System.DateTime(2016,01,01),
                    UserId = 10,
                    UserName = "Ben",
                    Version = 1,
                    Visible = true
                }
            });
            target.Pull();
            target.Flush();

            var command = new SqlCommand("select * from node where id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            var reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt64("id"));
            Assert.AreEqual(2 * 10000000, reader.GetInt64("latitude"));
            Assert.AreEqual(3 * 10000000, reader.GetInt64("longitude"));
            Assert.AreEqual(12, reader.GetInt64("changeset_id"));
            Assert.AreEqual(new System.DateTime(2016, 01, 01).ToUnixTime(), reader.GetInt64("timestamp"));
            Assert.AreEqual(10, reader.GetInt32("usr_id"));
            Assert.AreEqual("Ben", reader.GetString("usr"));
            Assert.AreEqual(1, reader.GetInt32("version"));
            Assert.AreEqual(true, reader.GetBoolean("visible"));

            command = new SqlCommand("select * from node_tags where node_id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            var key = reader.GetString("key");
            var value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
            Assert.IsTrue(reader.Read());
            key = reader.GetString("key");
            value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
        }

        /// <summary>
        /// Test loading one way.
        /// </summary>
        [Test]
        public void TestWriteWay()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestHistoryDb();
            var target = new OsmSharp.Db.SQLServer.Streams.HistoryDbStreamTarget(connection);

            target.RegisterSource(new OsmGeo[]
            {
                new Way()
                {
                    Id = 1,
                    Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                    Nodes = new long[]
                    {
                        12,
                        23,
                        34
                    },
                    ChangeSetId = 12,
                    TimeStamp = new System.DateTime(2016,01,01),
                    UserId = 10,
                    UserName = "Ben",
                    Version = 1,
                    Visible = true
                }
            });
            target.Pull();

            var command = new SqlCommand("select * from way where id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            var reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt64("id"));
            Assert.AreEqual(12, reader.GetInt64("changeset_id"));
            Assert.AreEqual(new System.DateTime(2016, 01, 01).ToUnixTime(), reader.GetInt64("timestamp"));
            Assert.AreEqual(10, reader.GetInt32("usr_id"));
            Assert.AreEqual("Ben", reader.GetString("usr"));
            Assert.AreEqual(1, reader.GetInt32("version"));
            Assert.AreEqual(true, reader.GetBoolean("visible"));

            command = new SqlCommand("select * from way_tags where way_id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            var key = reader.GetString("key");
            var value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
            Assert.IsTrue(reader.Read());
            key = reader.GetString("key");
            value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");

            command = new SqlCommand("select * from way_nodes where way_id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(0, reader.GetInt32("sequence_id"));
            Assert.AreEqual(12, reader.GetInt64("node_id"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32("sequence_id"));
            Assert.AreEqual(23, reader.GetInt64("node_id"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2, reader.GetInt32("sequence_id"));
            Assert.AreEqual(34, reader.GetInt64("node_id"));
        }

        /// <summary>
        /// Test loading one relation.
        /// </summary>
        [Test]
        public void TestWriteRelation()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestHistoryDb();
            var target = new OsmSharp.Db.SQLServer.Streams.HistoryDbStreamTarget(connection);

            target.RegisterSource(new OsmGeo[]
            {
                new Relation()
                {
                    Id = 1,
                    Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key0",
                            Value = "value0"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                    Members = new RelationMember[]
                    {
                        new RelationMember()
                        {
                            Id = 12,
                            Role = "first",
                            Type = OsmGeoType.Node
                        },
                        new RelationMember()
                        {
                            Id = 23,
                            Role = "second",
                            Type = OsmGeoType.Way
                        },
                        new RelationMember()
                        {
                            Id = 34,
                            Role = "third",
                            Type = OsmGeoType.Relation
                        }
                    },
                    ChangeSetId = 12,
                    TimeStamp = new System.DateTime(2016,01,01),
                    UserId = 10,
                    UserName = "Ben",
                    Version = 1,
                    Visible = true
                }
            });
            target.Pull();

            var command = new SqlCommand("select * from relation where id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            var reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt64("id"));
            Assert.AreEqual(12, reader.GetInt64("changeset_id"));
            Assert.AreEqual(new System.DateTime(2016, 01, 01).ToUnixTime(), reader.GetInt64("timestamp"));
            Assert.AreEqual(10, reader.GetInt32("usr_id"));
            Assert.AreEqual("Ben", reader.GetString("usr"));
            Assert.AreEqual(1, reader.GetInt32("version"));
            Assert.AreEqual(true, reader.GetBoolean("visible"));

            command = new SqlCommand("select * from relation_tags where relation_id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            var key = reader.GetString("key");
            var value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");
            Assert.IsTrue(reader.Read());
            key = reader.GetString("key");
            value = reader.GetString("value");
            Assert.IsTrue((key == "key0" && value == "value0") ||
                key == "key1" && value == "value1");

            command = new SqlCommand("select * from relation_members where relation_id = @id", connection);
            command.Parameters.AddWithValue("id", 1);
            reader = command.ExecuteReaderWrapper();
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(0, reader.GetInt32("sequence_id"));
            Assert.AreEqual(12, reader.GetInt64("member_id"));
            Assert.AreEqual(0, reader.GetInt32("member_type"));
            Assert.AreEqual("first", reader.GetString("member_role"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(1, reader.GetInt32("sequence_id"));
            Assert.AreEqual(23, reader.GetInt64("member_id"));
            Assert.AreEqual(1, reader.GetInt32("member_type"));
            Assert.AreEqual("second", reader.GetString("member_role"));
            Assert.IsTrue(reader.Read());
            Assert.AreEqual(2, reader.GetInt32("sequence_id"));
            Assert.AreEqual(34, reader.GetInt64("member_id"));
            Assert.AreEqual(2, reader.GetInt32("member_type"));
            Assert.AreEqual("third", reader.GetString("member_role"));
        }
    }
}