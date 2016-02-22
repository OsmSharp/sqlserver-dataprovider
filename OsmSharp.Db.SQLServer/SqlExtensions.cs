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

using OsmSharp.Tags;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer
{

    /// <summary>
    /// Contains sql extensions.
    /// </summary>
    public static class SqlExtensions
    {
        /// <summary>
        /// Adds a new parameter.
        /// </summary>
        public static void Add(this SqlParameterCollection parameters, string name, object value)
        {
            parameters.Add(new SqlParameter(name, value));
        }

        /// <summary>
        /// Builds a node from the current status of the reader.
        /// </summary>
        public static Node BuildNode(this DbDataReaderWrapper reader)
        {
            var node = new Node();
            reader.BuildNode(node);
            return node;
        }

        /// <summary>
        /// Builds a node from the current status of the reader.
        /// </summary>
        public static void BuildNode(this DbDataReaderWrapper reader, Node node)
        {
            reader.BuildOsmGeo(node);

            node.Latitude = (float)reader.GetInt64("latitude") / 10000000;
            node.Longitude = (float)reader.GetInt64("longitude") / 10000000;

            if (reader.HasColumn("node_id"))
            { // the tags column was joined in, also read the tags.
                reader.AddTags(node);
            }
        }

        /// <summary>
        /// Builds a way from the current status of the reader.
        /// </summary>
        public static Way BuildWay(this DbDataReaderWrapper reader)
        {
            var way = new Way();
            reader.BuildWay(way);
            return way;
        }

        /// <summary>
        /// Builds a way from the current status of the reader.
        /// </summary>
        public static void BuildWay(this DbDataReaderWrapper reader, Way way)
        {
            reader.BuildOsmGeo(way);

            if (reader.HasColumn("way_id") && reader.HasColumn("key"))
            { // the tags column was joined in, also read the tags.
                reader.AddTags(way);
            }
            else if (reader.HasColumn("way_id") && reader.HasColumn("sequence_id") && reader.HasColumn("node_id"))
            { // the tags column was joined in, also read the tags.
                reader.AddNodes(way);
            }
        }

        /// <summary>
        /// Builds a relation from the current status of the reader.
        /// </summary>
        public static Relation BuildRelation(this DbDataReaderWrapper reader)
        {
            var relation = new Relation();
            reader.BuildRelation(relation);
            return relation;
        }

        /// <summary>
        /// Builds a relation from the current status of the reader.
        /// </summary>
        public static void BuildRelation(this DbDataReaderWrapper reader, Relation relation)
        {
            reader.BuildOsmGeo(relation);

            if (reader.HasColumn("relation_id") && !reader.HasColumn("member_id"))
            { // the tags column was joined in, also read the tags.
                reader.AddTags(relation);
            }
            else if (reader.HasColumn("relation_id") && reader.HasColumn("member_id"))
            { // the members table was joined in, also read the members.
                reader.AddMembers(relation);
            }
        }

        /// <summary>
        /// Adds all the tags to the given node.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, Node node)
        {
            reader.AddTags(node, "node_id", "node_version");
        }

        /// <summary>
        /// Adds all the tags to the given way.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, Way way)
        {
            reader.AddTags(way, "way_id", "way_version");
        }

        /// <summary>
        /// Adds all the tags to the given relation.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, Relation relation)
        {
            reader.AddTags(relation, "relation_id", "relation_version");
        }

        /// <summary>
        /// Adds all the tags to the given osmGeo object.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, OsmGeo osmGeo, string idColumn, string versionColumn)
        {
            if (!reader.HasActiveRow)
            {
                return;
            }

            if (reader.IsDBNull(idColumn))
            { // no tags.
                reader.Read();
                return;
            }

            if (reader.HasActiveRow)
            {
                if (!reader.HasColumn(versionColumn))
                {
                    var id = reader.GetInt64(reader.GetOrdinal(idColumn));
                    osmGeo.Tags = new TagsCollection();
                    while (id == osmGeo.Id.Value)
                    {
                        osmGeo.Tags.Add(
                            reader.GetString("key"),
                            reader.GetString("value"));

                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        if (reader.IsDBNull(idColumn))
                        { // no tags anymore.
                            break;
                        }
                        id = reader.GetInt64(reader.GetOrdinal(idColumn));
                    }
                }
                else
                {
                    var id = reader.GetInt64(reader.GetOrdinal(idColumn));
                    var version = reader.GetInt32(versionColumn);
                    osmGeo.Tags = new TagsCollection();
                    while (id == osmGeo.Id.Value &&
                        version == osmGeo.Version.Value)
                    {
                        osmGeo.Tags.Add(
                            reader.GetString("key"),
                            reader.GetString("value"));

                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        if (reader.IsDBNull(idColumn))
                        { // no tags anymore.
                            break;
                        }
                        id = reader.GetInt64(reader.GetOrdinal(idColumn));
                        version = reader.GetInt32(versionColumn);
                    }
                }
            }
        }

        /// <summary>
        /// Builds an osmgeo object from the current status of the reader.
        /// </summary>
        public static void BuildOsmGeo(this DbDataReaderWrapper reader, OsmGeo osmGeo)
        {
            osmGeo.Id = reader.GetInt64("id");
            osmGeo.Version = reader.GetInt32("version");
            osmGeo.ChangeSetId = reader.GetInt64("changeset_id");
            osmGeo.TimeStamp = reader.GetInt64("timestamp").FromUnixTime();
            osmGeo.UserId = reader.GetInt32("usr_id");
            osmGeo.UserName = reader.GetString("usr");
            osmGeo.Visible = reader.GetBoolean("visible");
        }

        /// <summary>
        /// Adds all nodes to the given way.
        /// </summary>
        public static void AddNodes(this DbDataReaderWrapper reader, Way way)
        {
            if (reader.HasActiveRow)
            {
                if (!reader.HasColumn("way_version"))
                {
                    var wayId = reader.GetInt64("way_id");
                    var nodes = new List<long>();
                    while (wayId == way.Id.Value)
                    {
                        var sequenceId = reader.GetInt32("sequence_id");
                        if (nodes.Count != sequenceId)
                        {
                            throw new Exception(string.Format("Invalid sequence found in way_nodes for way {0}.",
                                way.Id.Value));
                        }
                        nodes.Add(reader.GetInt64("node_id"));
                        if (!reader.Read())
                        {
                            break;
                        }
                        wayId = reader.GetInt64("way_id");
                    }
                    way.Nodes = nodes.ToArray();
                }
                else
                {
                    var wayId = reader.GetInt64("way_id");
                    var version = reader.GetInt32("way_version");
                    var nodes = new List<long>();
                    while (wayId == way.Id.Value &&
                        version == way.Version.Value)
                    {
                        var sequenceId = reader.GetInt32("sequence_id");
                        if (nodes.Count != sequenceId)
                        {
                            throw new Exception(string.Format("Invalid sequence found in way_nodes for way {0}.",
                                way.Id.Value));
                        }
                        nodes.Add(reader.GetInt64("node_id"));
                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        wayId = reader.GetInt64("way_id");
                        version = reader.GetInt32("way_version");
                    }
                    way.Nodes = nodes.ToArray();
                }
            }
        }

        /// <summary>
        /// Adds all members to the given relation.
        /// </summary>
        public static void AddMembers(this DbDataReaderWrapper reader, Relation relation)
        {
            if (reader.HasActiveRow)
            {
                if (!reader.HasColumn("relation_version"))
                {
                    var relationId = reader.GetInt64("relation_id");
                    var members = new List<RelationMember>();
                    while (relationId == relation.Id.Value)
                    {
                        var sequenceId = reader.GetInt32("sequence_id");
                        if (members.Count != sequenceId)
                        {
                            throw new Exception(string.Format("Invalid sequence found in relation_members for relation {0}.",
                                relation.Id.Value));
                        }
                        var memberType = reader.GetInt32("member_type");
                        members.Add(
                            new RelationMember()
                            {
                                Id = reader.GetInt64("member_id"),
                                Role = reader.GetString("member_role"),
                                Type = (OsmGeoType)memberType
                            });
                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        relationId = reader.GetInt64("relation_id");
                    }
                    relation.Members = members.ToArray();
                }
                else
                {
                    var relationId = reader.GetInt64("relation_id");
                    var version = reader.GetInt32("relation_version");
                    var relationMembers = new List<RelationMember>();
                    while (relationId == relation.Id.Value &&
                        version == relation.Version.Value)
                    {
                        var memberType = reader.GetInt32("member_type");
                        relationMembers.Add(
                            new RelationMember()
                            {
                                Id = reader.GetInt64("member_id"),
                                Role = reader.GetString("member_role"),
                                Type = (OsmGeoType)memberType
                            });
                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        relationId = reader.GetInt64("relation_id");
                        version = reader.GetInt32("relation_version");
                    }
                    relation.Members = relationMembers.ToArray();
                }
            }
        }

        /// <summary>
        /// Executes a scalar and converts it to a long.
        /// </summary>
        public static long ExecuteScalarLong(this SqlCommand command, long dbNullValue = long.MinValue)
        {
            var obj = command.ExecuteScalar();
            if (obj == null || obj == DBNull.Value)
            {
                return dbNullValue;
            }
            if (obj is int)
            {
                return (int)obj;
            }
            return (long)obj;
        }

        /// <summary>
        /// Constructs a comma-seperated lists of the items in the given list.
        /// </summary>
        public static string BuildCommaSeperated<T>(this IList<T> values)
        {
            return SqlExtensions.BuildCommaSeperated(values, 0, values.Count);
        }

        /// <summary>
        /// Constructs an id list for SQL for only the specified section of ids.
        /// </summary>
        public static string BuildCommaSeperated<T>(this IList<T> values, int start, int count)
        {
            var stringBuilder = new System.Text.StringBuilder();
            if (values.Count > 0 && values.Count > start)
            {
                stringBuilder.Append(values[start].ToInvariantString());
                for (var i = start + 1; i < count + start; i++)
                {
                    stringBuilder.Append(',');
                    stringBuilder.Append(values[i].ToInvariantString());
                }
            }
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Constructs an id list for SQL for only the specified section of ids.
        /// </summary>
        public static string BuildCommaSeperated<T>(this IEnumerable<T> values)
        {
            var stringBuilder = new System.Text.StringBuilder();
            var isFirst = true;
            foreach (var value in values)
            {
                if (!isFirst)
                {
                    stringBuilder.Append(',');
                }
                stringBuilder.Append(value.ToInvariantString());
                isFirst = false;
            }
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Converts a standard DateTime into the number of milliseconds since 1/1/1970.
        /// </summary>
        public static object ToUnixTimeDB(this DateTime? date)
        {
            if (!date.HasValue)
            {
                return DBNull.Value;
            }
            return date.Value.ToUnixTime();
        }
    }
}