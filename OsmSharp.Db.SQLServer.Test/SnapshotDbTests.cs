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

using NUnit.Framework;
using OsmSharp.Changesets;
using OsmSharp.Tags;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer.Test
{
    /// <summary>
    /// Contains tests for the snapshot db.
    /// </summary>
    [TestFixture]
    public class SnapshotDbTests
    {
        /// <summary>
        /// Tests adding a node.
        /// </summary>
        [Test]
        public void TestAddNode()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Node()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

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
        /// Tests adding a way.
        /// </summary>
        [Test]
        public void TestAddWay()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Way()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

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
        /// Tests adding a relation.
        /// </summary>
        [Test]
        public void TestAddRelation()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Relation()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

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

        /// <summary>
        /// Tests getting a node.
        /// </summary>
        [Test]
        public void TestGetNode()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Node()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var node = db.GetNode(1);
            Assert.AreEqual(1, node.Id.Value);
            Assert.AreEqual(2, node.Latitude.Value);
            Assert.AreEqual(3, node.Longitude.Value);
            Assert.AreEqual(2, node.Tags.Count);
            Assert.IsTrue(node.Tags.Contains("key0", "value0"));
            Assert.IsTrue(node.Tags.Contains("key1", "value1"));
            Assert.AreEqual(12, node.ChangeSetId);
            Assert.AreEqual(new System.DateTime(2016, 01, 01), node.TimeStamp);
            Assert.AreEqual(10, node.UserId);
            Assert.AreEqual("Ben", node.UserName);
            Assert.AreEqual(1, node.Version);
            Assert.AreEqual(true, node.Visible);
        }

        /// <summary>
        /// Tests getting a way.
        /// </summary>
        [Test]
        public void TestGetWay()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Way()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var way = db.GetWay(1);
            Assert.AreEqual(1, way.Id.Value);
            Assert.AreEqual(2, way.Tags.Count);
            Assert.IsTrue(way.Tags.Contains("key0", "value0"));
            Assert.IsTrue(way.Tags.Contains("key1", "value1"));
            Assert.AreEqual(12, way.ChangeSetId);
            Assert.AreEqual(new System.DateTime(2016, 01, 01), way.TimeStamp);
            Assert.AreEqual(10, way.UserId);
            Assert.AreEqual("Ben", way.UserName);
            Assert.AreEqual(1, way.Version);
            Assert.AreEqual(true, way.Visible);
            var nodes = way.Nodes;
            Assert.IsNotNull(nodes);
            Assert.AreEqual(3, nodes.Length);
            Assert.AreEqual(12, way.Nodes[0]);
            Assert.AreEqual(23, way.Nodes[1]);
            Assert.AreEqual(34, way.Nodes[2]);
        }

        /// <summary>
        /// Tests adding a relation.
        /// </summary>
        [Test]
        public void TestGetRelation()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Relation()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var relation = db.GetRelation(1);
            Assert.AreEqual(1, relation.Id.Value);
            Assert.AreEqual(2, relation.Tags.Count);
            Assert.IsTrue(relation.Tags.Contains("key0", "value0"));
            Assert.IsTrue(relation.Tags.Contains("key1", "value1"));
            Assert.AreEqual(12, relation.ChangeSetId);
            Assert.AreEqual(new System.DateTime(2016, 01, 01), relation.TimeStamp);
            Assert.AreEqual(10, relation.UserId);
            Assert.AreEqual("Ben", relation.UserName);
            Assert.AreEqual(1, relation.Version);
            Assert.AreEqual(true, relation.Visible);
            var members = relation.Members;
            Assert.IsNotNull(members);
            Assert.AreEqual(3, members.Length);
            Assert.AreEqual(12, relation.Members[0].Id);
            Assert.AreEqual(OsmGeoType.Node, relation.Members[0].Type);
            Assert.AreEqual("first", relation.Members[0].Role);
            Assert.AreEqual(23, relation.Members[1].Id);
            Assert.AreEqual(OsmGeoType.Way, relation.Members[1].Type);
            Assert.AreEqual("second", relation.Members[1].Role);
            Assert.AreEqual(34, relation.Members[2].Id);
            Assert.AreEqual(OsmGeoType.Relation, relation.Members[2].Type);
            Assert.AreEqual("third", relation.Members[2].Role);
        }

        /// <summary>
        /// Tests clearing all data from the database.
        /// </summary>
        [Test]
        public void TestClear()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new Node()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });
            db.AddOrUpdate(new Way()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });
            db.AddOrUpdate(new Relation()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            db.Clear();

            var node = db.GetNode(1);
            Assert.IsNull(node);
            var way = db.GetWay(1);
            Assert.IsNull(way);
            var relation = db.GetRelation(1);
            Assert.IsNull(way);
        }

        /// <summary>
        /// Tests getting nodes in a box.
        /// </summary>
        [Test]
        public void TestGetNodesInBox()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 1,
                        Latitude = 1,
                        Longitude = 1,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    }
                });

            var result = new List<OsmGeo>(db.Get(0.9f, 0.9f, 1.1f, 1.1f));
            Assert.AreEqual(1, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            var node = result[0] as Node;
            Assert.AreEqual(1, node.Id);
            Assert.IsNull(node.Tags);

            connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            db = new SnapshotDb(connection);
            db.AddOrUpdate(new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 1,
                        Latitude = 1,
                        Longitude = 1,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Node()
                    {
                        Id = 2,
                        Latitude = 2,
                        Longitude = 2,
                        ChangeSetId = 12,
                        Tags = new TagsCollection(
                            new Tag("highway", "residential")),
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    }
                });

            result = new List<OsmGeo>(db.Get(0.9f, 0.9f, 2.1f, 2.1f));
            Assert.AreEqual(2, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            node = result[0] as Node;
            Assert.AreEqual(1, node.Id);
            Assert.IsTrue(node.Tags == null || node.Tags.Count == 0);
            Assert.IsInstanceOf<Node>(result[1]);
            node = result[1] as Node;
            Assert.AreEqual(2, node.Id);
            Assert.IsNotNull(node.Tags);
            Assert.IsTrue(node.Tags.Contains("highway", "residential"));
        }

        /// <summary>
        /// Tests getting ways in a box.
        /// </summary>
        [Test]
        public void TestGetWaysInBox()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 1,
                        Latitude = 1,
                        Longitude = 1,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Node()
                    {
                        Id = 2,
                        Latitude = 2,
                        Longitude = 2,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Node()
                    {
                        Id = 3,
                        Latitude = 3,
                        Longitude = 3,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Node()
                    {
                        Id = 4,
                        Latitude = 4,
                        Longitude = 4,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Way()
                    {
                        Id = 1,
                        ChangeSetId = 12,
                        Nodes = new long[] { 1, 2 },
                        Tags = new TagsCollection(
                            new Tag("highway", "residential")),
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Way()
                    {
                        Id = 2,
                        ChangeSetId = 12,
                        Nodes = new long[] { 3, 4 },
                        Tags = new TagsCollection(
                            new Tag("highway", "secondary")),
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    }
                });

            var result = new List<OsmGeo>(db.Get(0.9f, 0.9f, 1.1f, 1.1f));
            Assert.AreEqual(3, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            var node = result[0] as Node;
            Assert.AreEqual(1, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Node>(result[1]);
            node = result[1] as Node;
            Assert.AreEqual(2, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Way>(result[2]);
            var way = result[2] as Way;
            Assert.AreEqual(1, way.Id);
            Assert.IsNotNull(way.Tags);
            Assert.IsTrue(way.Tags.Contains("highway", "residential"));
            Assert.IsNotNull(way.Nodes);
            Assert.AreEqual(1, way.Nodes[0]);
            Assert.AreEqual(2, way.Nodes[1]);

            result = new List<OsmGeo>(db.Get(1.9f, 1.9f, 2.1f, 2.1f));
            Assert.AreEqual(3, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            node = result[0] as Node;
            Assert.AreEqual(1, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Node>(result[1]);
            node = result[1] as Node;
            Assert.AreEqual(2, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Way>(result[2]);
            way = result[2] as Way;
            Assert.AreEqual(1, way.Id);
            Assert.IsNotNull(way.Tags);
            Assert.IsTrue(way.Tags.Contains("highway", "residential"));
            Assert.IsNotNull(way.Nodes);
            Assert.AreEqual(1, way.Nodes[0]);
            Assert.AreEqual(2, way.Nodes[1]);

            result = new List<OsmGeo>(db.Get(1.9f, 1.9f, 3.1f, 3.1f));
            Assert.AreEqual(6, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            node = result[0] as Node;
            Assert.AreEqual(1, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Node>(result[1]);
            node = result[1] as Node;
            Assert.AreEqual(2, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Node>(result[2]);
            node = result[2] as Node;
            Assert.AreEqual(3, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Node>(result[3]);
            node = result[3] as Node;
            Assert.AreEqual(4, node.Id);
            Assert.IsNull(node.Tags);

            way = result[4] as Way;
            Assert.AreEqual(1, way.Id);
            Assert.IsNotNull(way.Tags);
            Assert.IsTrue(way.Tags.Contains("highway", "residential"));
            Assert.IsNotNull(way.Nodes);
            Assert.AreEqual(1, way.Nodes[0]);
            Assert.AreEqual(2, way.Nodes[1]);

            way = result[5] as Way;
            Assert.AreEqual(2, way.Id);
            Assert.IsNotNull(way.Tags);
            Assert.IsTrue(way.Tags.Contains("highway", "secondary"));
            Assert.IsNotNull(way.Nodes);
            Assert.AreEqual(3, way.Nodes[0]);
            Assert.AreEqual(4, way.Nodes[1]);
        }

        /// <summary>
        /// Tests getting relations in a box.
        /// </summary>
        [Test]
        public void TestGetRelationsInBox()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);
            db.AddOrUpdate(new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 1,
                        Latitude = 1,
                        Longitude = 1,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Node()
                    {
                        Id = 2,
                        Latitude = 2,
                        Longitude = 2,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Node()
                    {
                        Id = 3,
                        Latitude = 3,
                        Longitude = 3,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Node()
                    {
                        Id = 4,
                        Latitude = 4,
                        Longitude = 4,
                        ChangeSetId = 12,
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Way()
                    {
                        Id = 1,
                        ChangeSetId = 12,
                        Nodes = new long[] { 1, 2 },
                        Tags = new TagsCollection(
                            new Tag("highway", "residential")),
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Way()
                    {
                        Id = 2,
                        ChangeSetId = 12,
                        Nodes = new long[] { 3, 4 },
                        Tags = new TagsCollection(
                            new Tag("highway", "secondary")),
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Relation()
                    {
                        Id = 1,
                        ChangeSetId = 12,
                        Members = new RelationMember[]
                        {
                            new RelationMember()
                            {
                                Id = 1,
                                Role = "role1",
                                Type = OsmGeoType.Node
                            },
                            new RelationMember()
                            {
                                Id = 2,
                                Role = "role2",
                                Type = OsmGeoType.Node
                            }
                        },
                        Tags = new TagsCollection(
                            new Tag("type", "node_relation")),
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    },
                    new Relation()
                    {
                        Id = 2,
                        ChangeSetId = 12,
                        Members = new RelationMember[]
                        {
                            new RelationMember()
                            {
                                Id = 1,
                                Role = "role1",
                                Type = OsmGeoType.Way
                            },
                            new RelationMember()
                            {
                                Id = 2,
                                Role = "role2",
                                Type = OsmGeoType.Way
                            }
                        },
                        Tags = new TagsCollection(
                            new Tag("type", "way_relation")),
                        TimeStamp = new System.DateTime(2016, 01, 01),
                        UserId = 10,
                        UserName = "Ben",
                        Version = 1,
                        Visible = true
                    }
                });

            var result = new List<OsmGeo>(db.Get(0.9f, 0.9f, 1.1f, 1.1f));
            Assert.AreEqual(5, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            var node = result[0] as Node;
            Assert.AreEqual(1, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Node>(result[1]);
            node = result[1] as Node;
            Assert.AreEqual(2, node.Id);
            Assert.IsNull(node.Tags);
            Assert.IsInstanceOf<Way>(result[2]);
            var way = result[2] as Way;
            Assert.AreEqual(1, way.Id);
            Assert.IsNotNull(way.Tags);
            Assert.IsTrue(way.Tags.Contains("highway", "residential"));
            Assert.IsNotNull(way.Nodes);
            Assert.AreEqual(1, way.Nodes[0]);
            Assert.AreEqual(2, way.Nodes[1]);

            var relation = result[3] as Relation;
            Assert.AreEqual(1, relation.Id);
            Assert.IsNotNull(relation.Tags);
            Assert.IsTrue(relation.Tags.Contains("type", "node_relation"));
            Assert.IsNotNull(relation.Members);
            Assert.AreEqual(1, relation.Members[0].Id);
            Assert.AreEqual("role1", relation.Members[0].Role);
            Assert.AreEqual(OsmGeoType.Node, relation.Members[0].Type);
            Assert.AreEqual(2, relation.Members[1].Id);
            Assert.AreEqual("role2", relation.Members[1].Role);
            Assert.AreEqual(OsmGeoType.Node, relation.Members[1].Type);
        }

        /// <summary>
        /// Tests deleting a node.
        /// </summary>
        [Test]
        public void TestApplyChangesetDeleteNode()
        {
            var connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            var db = new SnapshotDb(connection);

            var node = new Node()
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
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            };
            db.AddOrUpdate(node);

            var changeset = new OsmChange()
            {
                Delete = new OsmGeo[]
                {
                    node
                }
            };

            db.ApplyChangeset(changeset);

            connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            db = new SnapshotDb(connection);

            node = new Node()
            {
                Id = 1,
                Latitude = 4,
                Longitude = 5,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key2",
                            Value = "value2"
                        },
                        new Tag()
                        {
                            Key = "key3",
                            Value = "value3"
                        }),
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            };
            db.AddOrUpdate(node);

            changeset = new OsmChange()
            {
                Modify = new OsmGeo[]
                {
                    node
                }
            };

            db.ApplyChangeset(changeset);

            connection = ConnectionHelper.GetConnectionToEmptyTestSnapshotDb();
            db = new SnapshotDb(connection);

            node = new Node()
            {
                Id = -1,
                Latitude = 4,
                Longitude = 5,
                Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key2",
                            Value = "value2"
                        },
                        new Tag()
                        {
                            Key = "key3",
                            Value = "value3"
                        }),
                ChangeSetId = 12,
                TimeStamp = new System.DateTime(2016, 01, 01),
                UserId = 10,
                UserName = "Ben",
                Version = 1,
                Visible = true
            };

            changeset = new OsmChange()
            {
                Create = new OsmGeo[]
                {
                    node
                }
            };

            db.ApplyChangeset(changeset);
        }
    }
}