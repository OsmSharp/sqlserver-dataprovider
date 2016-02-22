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
using OsmSharp.Streams;
using OsmSharp.Tags;
using System.Collections.Generic;

namespace OsmSharp.Db.SQLServer.Test.Streams
{
    /// <summary>
    /// Containts tests for the history db stream source.
    /// </summary>
    [TestFixture]
    public class HistoryDbStreamSourceTests
    {
        /// <summary>
        /// Test reading one node.
        /// </summary>
        [Test]
        public void TestReadNode()
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

            // test source.
            var source = new OsmSharp.Db.SQLServer.Streams.HistoryDbStreamSource(connection);
            var result = new List<OsmGeo>(source);
            Assert.AreEqual(1, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            var node = result[0] as Node;
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
        /// Test reading one way.
        /// </summary>
        [Test]
        public void TestReadWay()
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

            // test source.
            var source = new OsmSharp.Db.SQLServer.Streams.HistoryDbStreamSource(connection);
            var result = new List<OsmGeo>(source);
            Assert.AreEqual(1, result.Count);
            Assert.IsInstanceOf<Way>(result[0]);
            var way = result[0] as Way;
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
        /// Test reading one relation.
        /// </summary>
        [Test]
        public void TestReadRelation()
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

            // test source.
            var source = new OsmSharp.Db.SQLServer.Streams.HistoryDbStreamSource(connection);
            var result = new List<OsmGeo>(source);
            Assert.AreEqual(1, result.Count);
            Assert.IsInstanceOf<Relation>(result[0]);
            var relation = result[0] as Relation;
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
        /// Test reading a small collection of data.
        /// </summary>
        [Test]
        public void TestRead()
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
                },
                new Node()
                {
                    Id = 2,
                    Latitude = 20,
                    Longitude = 30,
                    Tags = new TagsCollection(
                        new Tag()
                        {
                            Key = "key2",
                            Value = "value2"
                        },
                        new Tag()
                        {
                            Key = "key1",
                            Value = "value1"
                        }),
                    ChangeSetId = 14,
                    TimeStamp = new System.DateTime(2016,01,01),
                    UserId = 11,
                    UserName = "Ben1",
                    Version = 1,
                    Visible = true
                },
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
                },
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

            // test source.
            var source = new OsmSharp.Db.SQLServer.Streams.HistoryDbStreamSource(connection);
            var result = new List<OsmGeo>(source);
            Assert.AreEqual(4, result.Count);
            Assert.IsInstanceOf<Node>(result[0]);
            var node = result[0] as Node;
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
            Assert.IsInstanceOf<Node>(result[1]);

            node = result[1] as Node;
            Assert.AreEqual(2, node.Id.Value);
            Assert.AreEqual(20, node.Latitude.Value);
            Assert.AreEqual(30, node.Longitude.Value);
            Assert.AreEqual(2, node.Tags.Count);
            Assert.IsTrue(node.Tags.Contains("key2", "value2"));
            Assert.IsTrue(node.Tags.Contains("key1", "value1"));
            Assert.AreEqual(14, node.ChangeSetId);
            Assert.AreEqual(new System.DateTime(2016, 01, 01), node.TimeStamp);
            Assert.AreEqual(11, node.UserId);
            Assert.AreEqual("Ben1", node.UserName);
            Assert.AreEqual(1, node.Version);
            Assert.AreEqual(true, node.Visible);

            Assert.IsInstanceOf<Way>(result[2]);
            var way = result[2] as Way;
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

            Assert.IsInstanceOf<Relation>(result[3]);
            var relation = result[3] as Relation;
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
    }
}