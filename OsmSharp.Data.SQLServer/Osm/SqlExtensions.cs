// OsmSharp - OpenStreetMap (OSM) SDK
//
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

using OsmSharp.Collections.Tags;
using OsmSharp.Osm;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace OsmSharp.Data.SQLServer.Osm
{
    /// <summary>
    /// Contains extension methods for Sql related classes.
    /// </summary>
    public static class SqlExtensions
    {
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
        /// Builds a way from the current status of the reader.
        /// </summary>
        public static void BuildNode(this DbDataReaderWrapper reader, Node node)
        {
            reader.BuildOsmGeo(node);

            node.Latitude = (double)reader.GetInt32("latitude") / 10000000;
            node.Longitude = (double)reader.GetInt32("longitude") / 10000000;
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
        }

        /// <summary>
        /// Adds all the tags to the given node.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, Node node)
        {
            reader.AddTags(node, "node_id");
        }

        /// <summary>
        /// Adds all the tags to the given way.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, Way way)
        {
            reader.AddTags(way, "way_id");
        }

        /// <summary>
        /// Adds all the tags to the given relation.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, Relation relation)
        {
            reader.AddTags(relation, "relation_id");
        }

        /// <summary>
        /// Adds all the tags to the given osmGeo object.
        /// </summary>
        public static void AddTags(this DbDataReaderWrapper reader, OsmGeo osmGeo, string idColumn)
        {
            if (reader.HasActiveRow)
            {
                if (!reader.HasColumn("version"))
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
                        id = reader.GetInt64(reader.GetOrdinal(idColumn));
                    }
                }
                else
                {
                    var id = reader.GetInt64(reader.GetOrdinal(idColumn));
                    var version = reader.GetInt32("version");
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
                        id = reader.GetInt64(reader.GetOrdinal(idColumn));
                        version = reader.GetInt32("version");
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
            osmGeo.TimeStamp = reader.GetDateTime("timestamp");
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
                if (!reader.HasColumn("version"))
                {
                    var wayId = reader.GetInt64("way_id");
                    way.Nodes = new List<long>();
                    while (wayId == way.Id.Value)
                    {
                        var sequenceId = reader.GetInt32("sequence_id");
                        if (way.Nodes.Count != sequenceId)
                        {
                            throw new Exception(string.Format("Invalid sequence found in way_nodes for way {0}.",
                                way.Id.Value));
                        }
                        way.Nodes.Add(reader.GetInt64("node_id"));
                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        wayId = reader.GetInt64("way_id");
                    }
                }
                else
                {
                    var wayId = reader.GetInt64("way_id");
                    var version = reader.GetInt32("version");
                    way.Nodes = new List<long>();
                    while (wayId == way.Id.Value &&
                        version == way.Version.Value)
                    {
                        var sequenceId = reader.GetInt32("sequence_id");
                        if (way.Nodes.Count != sequenceId)
                        {
                            throw new Exception(string.Format("Invalid sequence found in way_nodes for way {0}.",
                                way.Id.Value));
                        }
                        way.Nodes.Add(reader.GetInt64("node_id"));
                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        wayId = reader.GetInt64("way_id");
                        version = reader.GetInt32("version");
                    }
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
                if (!reader.HasColumn("version"))
                {
                    var relationId = reader.GetInt64("relation_id");
                    relation.Members = new List<RelationMember>();
                    while (relationId == relation.Id.Value)
                    {
                        var sequenceId = reader.GetInt32("sequence_id");
                        if (relation.Members.Count != sequenceId)
                        {
                            throw new Exception(string.Format("Invalid sequence found in relation_members for relation {0}.",
                                relation.Id.Value));
                        }
                        var memberType = reader.GetInt32("member_type");
                        relation.Members.Add(
                            new RelationMember()
                            {
                                MemberId = reader.GetInt64("member_id"),
                                MemberRole = reader.GetString("member_role"),
                                MemberType = (OsmGeoType)memberType
                            });
                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        relationId = reader.GetInt64("relation_id");
                    }
                }
                else
                {
                    var relationId = reader.GetInt64("relation_id");
                    var version = reader.GetInt32("version");
                    relation.Members = new List<RelationMember>();
                    while (relationId == relation.Id.Value &&
                        version == relation.Version.Value)
                    {
                        var sequenceId = reader.GetInt32("sequence_id");
                        if (relation.Members.Count != sequenceId)
                        {
                            throw new Exception(string.Format("Invalid sequence found in relation_members for relation {0}.",
                                relation.Id.Value));
                        }
                        var memberType = reader.GetInt32("member_type");
                        relation.Members.Add(
                            new RelationMember()
                            {
                                MemberId = reader.GetInt64("member_id"),
                                MemberRole = reader.GetString("member_role"),
                                MemberType = (OsmGeoType)memberType
                            });
                        if (!reader.Read())
                        { // move to next record.
                            break;
                        }
                        relationId = reader.GetInt64("relation_id");
                        version = reader.GetInt32("version");
                    }
                }
            }
        }
    }
}