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

using OsmSharp.Changesets;
using OsmSharp.Tags;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace OsmSharp.Db.SQLServer
{

    /// <summary>
    /// Contains sql extensions.
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
        /// Builds a changeset from the current status of the reader.
        /// </summary>
        internal static Changeset BuildChangeset(this DbDataReaderWrapper reader)
        {
            var changeset = new Changeset();
            reader.BuildChangeset(changeset);
            return changeset;
        }

        /// <summary>
        /// Builds a changetset from the current status of the reader.
        /// </summary>
        internal static void BuildChangeset(this DbDataReaderWrapper reader, Changeset changeset)
        {
            changeset.Id = reader.GetInt64("id");
            changeset.ClosedAt = reader.GetUnixDateTime("closed_at");
            changeset.CreatedAt = reader.GetUnixDateTime("created_at");
            changeset.MinLatitude = (float)reader.GetInt32("min_lat") / 10000000;
            changeset.MinLongitude = (float)reader.GetInt32("min_lon") / 10000000;
            changeset.MaxLatitude = (float)reader.GetInt32("max_lat") / 10000000;
            changeset.MaxLongitude = (float)reader.GetInt32("max_lon") / 10000000;
            changeset.UserId = reader.GetInt32("usr_id");
            changeset.Open = changeset.ClosedAt == null;
        }

        /// <summary>
        /// Adds all the tags to the given changeset.
        /// </summary>
        internal static void AddTags(this DbDataReaderWrapper reader, Changeset changeset)
        {
            var idColumn = "changeset_id";

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
                var id = reader.GetInt64(reader.GetOrdinal(idColumn));
                changeset.Tags = new TagsCollection();
                while (id == changeset.Id.Value)
                {
                    changeset.Tags.Add(
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
        /// Constructs an id list for SQL for only the specified section of ids.
        /// </summary>
        public static string BuildCommaSeperatedWithRoundBrackets<T>(this IEnumerable<T> values)
        {
            var stringBuilder = new System.Text.StringBuilder();
            var isFirst = true;
            foreach (var value in values)
            {
                if (!isFirst)
                {
                    stringBuilder.Append(',');
                }
                stringBuilder.Append('(');
                stringBuilder.Append(value.ToInvariantString());
                stringBuilder.Append(')');
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

        /// <summary>
        /// Gets a command for the connection.
        /// </summary>
        internal static SqlCommand GetCommand(this SqlConnection connection, string commandText)
        {
            return new SqlCommand(commandText, connection);
        }

        /// <summary>
        /// Gets a command for the connection.
        /// </summary>
        internal static SqlCommand GetCommand(this SqlConnection connection, string commandText, params object[] args)
        {
            return new SqlCommand(string.Format(commandText, args), connection);
        }

        /// <summary>
        /// Adds or updates a changeset.
        /// </summary>
        internal static void AddOrUpdate(this SqlConnection connection, Changesets.Changeset changeset)
        {
            var cmd = connection.GetCommand("delete from changeset_tags where changeset_id = @changeset_id");
            cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = connection.GetCommand("delete from changeset_changes where changeset_id = @changeset_id");
            cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = connection.GetCommand(
                "update changeset set " +
                "closed_at=@closed_at, created_at=@created_at, " +
                "min_lat=@min_lat, min_lon=@min_lon, max_lat=@max_lat, max_lon=@max_lon, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", changeset.Id.Value);
            cmd.Parameters.AddWithValue("closed_at", changeset.ClosedAt.ToUnixTimeDB());
            cmd.Parameters.AddWithValue("created_at", changeset.CreatedAt.ToUnixTimeDB());
            cmd.Parameters.AddWithValue("min_lat", (int)(changeset.MinLatitude.Value * 10000000));
            cmd.Parameters.AddWithValue("min_lon", (int)(changeset.MinLongitude.Value * 10000000));
            cmd.Parameters.AddWithValue("max_lat", (int)(changeset.MaxLatitude.Value * 10000000));
            cmd.Parameters.AddWithValue("max_lon", (int)(changeset.MaxLongitude.Value * 10000000));
            cmd.Parameters.AddWithValue("usr_id", changeset.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = connection.GetCommand(
                    "INSERT INTO changeset (id, closed_at, created_at, min_lat, min_lon, max_lat, max_lon, usr_id) " +
                       "VALUES(@id, @closed_at, @created_at, @min_lat, @min_lon, @max_lat, @max_lon, @usr_id)");
                cmd.Parameters.AddWithValue("id", changeset.Id.Value);
                cmd.Parameters.AddWithValue("closed_at", changeset.ClosedAt.ToUnixTimeDB());
                cmd.Parameters.AddWithValue("created_at", changeset.CreatedAt.ToUnixTimeDB());
                cmd.Parameters.AddWithValue("min_lat", (int)(changeset.MinLatitude.Value * 10000000));
                cmd.Parameters.AddWithValue("min_lon", (int)(changeset.MinLongitude.Value * 10000000));
                cmd.Parameters.AddWithValue("max_lat", (int)(changeset.MaxLatitude.Value * 10000000));
                cmd.Parameters.AddWithValue("max_lon", (int)(changeset.MaxLongitude.Value * 10000000));
                cmd.Parameters.AddWithValue("usr_id", changeset.UserId.Value);

                cmd.ExecuteNonQuery();
            }

            if (changeset.Tags != null)
            {
                cmd = connection.GetCommand("INSERT into changeset_tags (changeset_id, [key], value) VALUES (@changeset_id, @key, @value)");
                cmd.Parameters.AddWithValue("changeset_id", changeset.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);

                foreach (var tag in changeset.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds the given changes for the given changeset.
        /// </summary>
        internal static void AddChanges(this SqlConnection connection, long changesetId, Changesets.OsmChange osmChange)
        {
            if (osmChange.Create != null)
            {
                foreach (var create in osmChange.Create)
                {
                    var cmd = connection.GetCommand(
                        "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                           "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                    cmd.Parameters.AddWithValue("changeset_id", changesetId);
                    cmd.Parameters.AddWithValue("type", Constants.CreateType);
                    cmd.Parameters.AddWithValue("osm_id", create.Id.Value);
                    cmd.Parameters.AddWithValue("osm_type", (int)create.Type);
                    cmd.Parameters.AddWithValue("osm_version", create.Version.Value);

                    cmd.ExecuteNonQuery();
                }
            }

            if (osmChange.Delete != null)
            {
                foreach (var delete in osmChange.Delete)
                {
                    var cmd = connection.GetCommand(
                        "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                           "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                    cmd.Parameters.AddWithValue("changeset_id", changesetId);
                    cmd.Parameters.AddWithValue("type", Constants.DeleteType);
                    cmd.Parameters.AddWithValue("osm_id", delete.Id.Value);
                    cmd.Parameters.AddWithValue("osm_type", (int)delete.Type);
                    cmd.Parameters.AddWithValue("osm_version", delete.Version.Value);

                    cmd.ExecuteNonQuery();
                }
            }

            if (osmChange.Modify != null)
            {
                foreach (var modify in osmChange.Modify)
                {
                    var cmd = connection.GetCommand(
                        "INSERT INTO changeset_changes (changeset_id, type, osm_id, osm_type, osm_version) " +
                           "VALUES(@changeset_id, @type, @osm_id, @osm_type, @osm_version)");
                    cmd.Parameters.AddWithValue("changeset_id", changesetId);
                    cmd.Parameters.AddWithValue("type", Constants.ModifyType);
                    cmd.Parameters.AddWithValue("osm_id", modify.Id.Value);
                    cmd.Parameters.AddWithValue("osm_type", (int)modify.Type);
                    cmd.Parameters.AddWithValue("osm_version", modify.Version.Value);

                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds or updates an osm geo object.
        /// </summary>
        internal static bool AddOrUpdate(this SqlConnection connection, OsmGeo osmGeo)
        {
            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    return connection.AddOrUpdateNode(osmGeo as Node);
                case OsmGeoType.Way:
                    return connection.AddOrUpdateWay(osmGeo as Way);
                case OsmGeoType.Relation:
                    return connection.AddOrUpdateRelation(osmGeo as Relation);
            }
            return false;
        }

        /// <summary>
        /// Adds or updates a node.
        /// </summary>
        internal static bool AddOrUpdateNode(this SqlConnection connection, Node node)
        {
            var wasUpdate = true;
            var cmd = connection.GetCommand("delete from node_tags where node_id = @node_id");
            cmd.Parameters.AddWithValue("node_id", node.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = connection.GetCommand("update node set latitude=@latitude, longitude=@longitude, changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, tile=@tile, version=@version, usr=@usr, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
            cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
            cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", node.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("tile", (long)Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id);
            cmd.Parameters.AddWithValue("version", node.Version.Value);
            cmd.Parameters.AddWithValue("usr", node.UserName);
            cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);
            cmd.Parameters.AddWithValue("id", node.Id.Value);

            if (cmd.ExecuteNonQuery() == 0)
            { // oeps, node did not exist, insert.
                cmd = connection.GetCommand(
                    "insert into node (id, latitude, longitude, changeset_id, visible, timestamp, tile, version, usr, usr_id) " +
                             "values(@id, @latitude, @longitude, @changeset_id, @visible, @timestamp, @tile, @version, @usr, @usr_id)");
                cmd.Parameters.AddWithValue("id", node.Id.Value);
                cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
                cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
                cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", node.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("tile", (long)Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id);
                cmd.Parameters.AddWithValue("version", node.Version.Value);
                cmd.Parameters.AddWithValue("usr", node.UserName);
                cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);

                cmd.ExecuteNonQuery();
                wasUpdate = false;
            }

            if (node.Tags != null)
            {
                cmd = connection.GetCommand("INSERT into node_tags (node_id, [key], value) VALUES (@node_id, @key, @value)");
                cmd.Parameters.AddWithValue("node_id", node.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in node.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }
            return wasUpdate;
        }

        /// <summary>
        /// Adds or updates a way.
        /// </summary>
        internal static bool AddOrUpdateWay(this SqlConnection connection, Way way)
        {
            var wasUpdate = true;
            var cmd = connection.GetCommand("delete from way_tags where way_id = @way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = connection.GetCommand("delete from way_nodes where way_id = @way_id");
            cmd.Parameters.AddWithValue("way_id", way.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = connection.GetCommand(
                "update way set " +
                "changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, " +
                "version=@version, usr=@usr, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", way.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", way.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", way.Version.Value);
            cmd.Parameters.AddWithValue("usr", way.UserName);
            cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = connection.GetCommand(
                    "INSERT INTO way (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                       "VALUES(@id, @changeset_id, @visible, @timestamp, @version, @usr, @usr_id)");
                cmd.Parameters.AddWithValue("id", way.Id.Value);
                cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", way.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("version", way.Version.Value);
                cmd.Parameters.AddWithValue("usr", way.UserName);
                cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);

                cmd.ExecuteNonQuery();
                wasUpdate = false;
            }

            if (way.Tags != null)
            {
                cmd = connection.GetCommand("INSERT into way_tags (way_id, [key], value) VALUES (@way_id, @key, @value)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in way.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (way.Nodes != null)
            {
                cmd = connection.GetCommand("INSERT into way_nodes (way_id, node_id, sequence_id) VALUES (@way_id, @node_id, @sequence_id)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.Add("node_id", SqlDbType.BigInt);
                cmd.Parameters.Add("sequence_id", SqlDbType.BigInt);
                for (var i = 0; i < way.Nodes.Length; i++)
                {
                    cmd.Parameters["node_id"].Value = way.Nodes[i];
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
            return wasUpdate;
        }

        /// <summary>
        /// Adds or updates a relation.
        /// </summary>
        internal static bool AddOrUpdateRelation(this SqlConnection connection, Relation relation)
        {
            var wasUpdate = true;
            var cmd = connection.GetCommand("delete from relation_tags where relation_id = @relation_id");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = connection.GetCommand("delete from relation_members where relation_id = @relation_id");
            cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
            cmd.ExecuteNonQuery();

            cmd = connection.GetCommand(
                "update relation set " +
                "changeset_id=@changeset_id, " +
                "visible=@visible, timestamp=@timestamp, " +
                "version=@version, usr=@usr, usr_id=@usr_id " +
                "where id=@id");
            cmd.Parameters.AddWithValue("id", relation.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", relation.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", relation.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", relation.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", relation.Version.Value);
            cmd.Parameters.AddWithValue("usr", relation.UserName);
            cmd.Parameters.AddWithValue("usr_id", relation.UserId.Value);

            if (cmd.ExecuteNonQuery() == 0)
            {
                cmd = connection.GetCommand(
                    "INSERT INTO relation (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                       "VALUES(@id, @changeset_id, @visible, @timestamp, @version, @usr, @usr_id)");
                cmd.Parameters.AddWithValue("id", relation.Id.Value);
                cmd.Parameters.AddWithValue("changeset_id", relation.ChangeSetId.Value);
                cmd.Parameters.AddWithValue("visible", relation.Visible.Value);
                cmd.Parameters.AddWithValue("timestamp", relation.TimeStamp.Value.ToUnixTime());
                cmd.Parameters.AddWithValue("version", relation.Version.Value);
                cmd.Parameters.AddWithValue("usr", relation.UserName);
                cmd.Parameters.AddWithValue("usr_id", relation.UserId.Value);

                cmd.ExecuteNonQuery();
                wasUpdate = false;
            }

            if (relation.Tags != null)
            {
                cmd = connection.GetCommand("INSERT into relation_tags (relation_id, [key], value) VALUES (@relation_id, @key, @value)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in relation.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (relation.Members != null)
            {
                cmd = connection.GetCommand("INSERT into relation_members (relation_id, member_id, member_type, member_role, sequence_id) VALUES (@relation_id, @member_id, @member_type, @member_role, @sequence_id)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("member_id", SqlDbType.BigInt);
                cmd.Parameters.Add("member_type", SqlDbType.Int);
                cmd.Parameters.Add("member_role", SqlDbType.Text);
                cmd.Parameters.Add("sequence_id", SqlDbType.Int);
                for (var i = 0; i < relation.Members.Length; i++)
                {
                    cmd.Parameters["member_id"].Value = relation.Members[i].Id;
                    cmd.Parameters["member_role"].Value = relation.Members[i].Role;
                    cmd.Parameters["member_type"].Value = (int)relation.Members[i].Type;
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
            return wasUpdate;
        }

        /// <summary>
        /// Adds an archived osmGeo.
        /// </summary>
        internal static void AddArchive(this SqlConnection connection, OsmGeo osmGeo)
        {
            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    connection.AddArchiveNode(osmGeo as Node);
                    break;
                case OsmGeoType.Way:
                    connection.AddArchiveWay(osmGeo as Way);
                    break;
                case OsmGeoType.Relation:
                    connection.AddArchiveRelation(osmGeo as Relation);
                    break;
            }
        }

        /// <summary>
        /// Adds an archived node.
        /// </summary>
        internal static void AddArchiveNode(this SqlConnection connection, Node node)
        {
            var cmd = connection.GetCommand(
                "insert into archived_node (id, latitude, longitude, changeset_id, visible, timestamp, tile, version, usr, usr_id) " +
                         "values (@id, @latitude, @longitude, @changeset_id, @visible, @timestamp, @tile, @version, @usr, @usr_id)");
            cmd.Parameters.AddWithValue("id", node.Id.Value);
            cmd.Parameters.AddWithValue("latitude", (int)(node.Latitude.Value * 10000000));
            cmd.Parameters.AddWithValue("longitude", (int)(node.Longitude.Value * 10000000));
            cmd.Parameters.AddWithValue("changeset_id", node.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", node.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", node.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("tile", Tiles.Tile.CreateAroundLocation(node.Latitude.Value, node.Longitude.Value, Constants.DefaultZoom).Id);
            cmd.Parameters.AddWithValue("version", node.Version.Value);
            cmd.Parameters.AddWithValue("usr", node.UserName);
            cmd.Parameters.AddWithValue("usr_id", node.UserId.Value);
            cmd.ExecuteNonQuery();

            if (node.Tags != null)
            {
                cmd = connection.GetCommand("insert into archived_node_tags (node_id, node_version, [key], value) " +
                    "values (@node_id, @node_version, @key, @value)");
                cmd.Parameters.AddWithValue("node_id", node.Id.Value);
                cmd.Parameters.AddWithValue("node_version", node.Version.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in node.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds an archived way.
        /// </summary>
        internal static void AddArchiveWay(this SqlConnection connection, Way way)
        {
            var cmd = connection.GetCommand(
                "insert into archived_way (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                   "values (@id, @changeset_id, @visible, @timestamp, @version, @usr, @usr_id)");
            cmd.Parameters.AddWithValue("id", way.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", way.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", way.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", way.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", way.Version.Value);
            cmd.Parameters.AddWithValue("usr", way.UserName);
            cmd.Parameters.AddWithValue("usr_id", way.UserId.Value);
            cmd.ExecuteNonQuery();

            if (way.Tags != null)
            {
                cmd = connection.GetCommand("insert into archived_way_tags (way_id, way_version, [key], value) values " +
                    "(@way_id, @way_version, @key, @value)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.AddWithValue("way_version", way.Version.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in way.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (way.Nodes != null)
            {
                cmd = connection.GetCommand("insert into archived_way_nodes (way_id, way_version, node_id, sequence_id) values " +
                    "(@way_id, @way_version, @node_id, @sequence_id)");
                cmd.Parameters.AddWithValue("way_id", way.Id.Value);
                cmd.Parameters.AddWithValue("way_version", way.Version.Value);
                cmd.Parameters.Add("node_id", SqlDbType.BigInt);
                cmd.Parameters.Add("sequence_id", SqlDbType.BigInt);
                for (var i = 0; i < way.Nodes.Length; i++)
                {
                    cmd.Parameters["node_id"].Value = way.Nodes[i];
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Adds an archived relation.
        /// </summary>
        internal static void AddArchiveRelation(this SqlConnection connection, Relation relation)
        {
            var cmd = connection.GetCommand(
                "insert into archived_relation (id, changeset_id, visible, timestamp, version, usr, usr_id) " +
                   "values (@id, @changeset_id, @visible, @timestamp, @version, @usr, @usr_id)");
            cmd.Parameters.AddWithValue("id", relation.Id.Value);
            cmd.Parameters.AddWithValue("changeset_id", relation.ChangeSetId.Value);
            cmd.Parameters.AddWithValue("visible", relation.Visible.Value);
            cmd.Parameters.AddWithValue("timestamp", relation.TimeStamp.Value.ToUnixTime());
            cmd.Parameters.AddWithValue("version", relation.Version.Value);
            cmd.Parameters.AddWithValue("usr", relation.UserName);
            cmd.Parameters.AddWithValue("usr_id", relation.UserId.Value);
            cmd.ExecuteNonQuery();

            if (relation.Tags != null)
            {
                cmd = connection.GetCommand("insert into archived_relation_tags (relation_id, [key], value) values " +
                    "(@relation_id, @key, @value)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("key", SqlDbType.Text);
                cmd.Parameters.Add("value", SqlDbType.Text);
                foreach (var tag in relation.Tags)
                {
                    cmd.Parameters["key"].Value = tag.Key;
                    cmd.Parameters["value"].Value = tag.Value;
                    cmd.ExecuteNonQuery();
                }
            }

            if (relation.Members != null)
            {
                cmd = connection.GetCommand("insert into archived_relation_members (relation_id, member_id, member_type, member_role, sequence_id) values " +
                    "(@relation_id, @member_id, @member_type, @member_role, @sequence_id)");
                cmd.Parameters.AddWithValue("relation_id", relation.Id.Value);
                cmd.Parameters.Add("member_id", SqlDbType.BigInt);
                cmd.Parameters.Add("member_type", SqlDbType.Int);
                cmd.Parameters.Add("member_role", SqlDbType.Text);
                cmd.Parameters.Add("sequence_id", SqlDbType.Int);
                for (var i = 0; i < relation.Members.Length; i++)
                {
                    cmd.Parameters["member_id"].Value = relation.Members[i].Id;
                    cmd.Parameters["member_role"].Value = relation.Members[i].Role;
                    cmd.Parameters["member_type"].Value = (int)relation.Members[i].Type;
                    cmd.Parameters["sequence_id"].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Deletes all nodes with the given ids.
        /// </summary>
        internal static void DeleteNodesById(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return;
            }

            var command = connection.GetCommand("DELETE FROM node_tags WHERE node_id IN ({0})", idsList);
            command.ExecuteNonQuery();

            command = connection.GetCommand("DELETE FROM node WHERE id IN ({0})", idsList);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Deletes all ways with the given ids.
        /// </summary>
        internal static void DeleteWaysById(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return;
            }

            var command = connection.GetCommand("DELETE FROM way_tags WHERE way_id IN ({0})", idsList);
            command.ExecuteNonQuery();

            command = connection.GetCommand("DELETE FROM way_nodes WHERE way_id IN ({0})", idsList);
            command.ExecuteNonQuery();

            command = connection.GetCommand("DELETE FROM way WHERE id IN ({0})", idsList);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Deletes all relations with the given ids.
        /// </summary>
        internal static void DeleteRelationsById(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return;
            }

            var command = connection.GetCommand("DELETE FROM relation_tags WHERE relation_id IN ({0})", idsList);
            command.ExecuteNonQuery();

            command = connection.GetCommand("DELETE FROM relation_members WHERE relation_id IN ({0})", idsList);
            command.ExecuteNonQuery();

            command = connection.GetCommand("DELETE FROM relation WHERE id IN ({0})", idsList);
            command.ExecuteNonQuery();
        }

        /// <summary>
        /// Gets all nodes with the given ids.
        /// </summary>
        internal static IEnumerable<Node> GetNodesById(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return Enumerable.Empty<Node>();
            }

            var nodeTagsCommand = connection.GetCommand("select * from node_tags WHERE node_id IN ({0}) order by node_id", idsList);
            var nodeCommand = connection.GetCommand("select * from node WHERE id IN ({0}) order by id", idsList);

            return new Enumerators.NodeEnumerable(nodeCommand, nodeTagsCommand);
        }

        /// <summary>
        /// Gets archived node.
        /// </summary>
        internal static Node GetArchivedNode(this SqlConnection connection, long id, int version)
        {
            var tagsCommand = connection.GetCommand("select * from archived_node_tags where node_id={0} and node_version={1} order by node_id",
                id.ToInvariantString(), version.ToInvariantString());
            var command = connection.GetCommand("select * from archived_node where id={0} and version={1} order by id",
                id.ToInvariantString(), version.ToInvariantString());

            return new Enumerators.NodeEnumerable(command, tagsCommand).FirstOrDefault();
        }

        /// <summary>
        /// Gets archived way.
        /// </summary>
        internal static Way GetArchivedWay(this SqlConnection connection, long id, int version)
        {
            var tagsCommand = connection.GetCommand("select * from archived_way_tags where way_id={0} and way_version={1} order by way_id",
                id.ToInvariantString(), version.ToInvariantString());
            var nodesCommand = connection.GetCommand("select * from archived_way_nodes where way_id={0} and way_version={1} order by way_id, sequence_id",
                id.ToInvariantString(), version.ToInvariantString());
            var command = connection.GetCommand("select * from archived_way where id={0} and version={1} order by id",
                id.ToInvariantString(), version.ToInvariantString());

            return new Enumerators.WayEnumerable(command, tagsCommand, nodesCommand).FirstOrDefault();
        }

        /// <summary>
        /// Gets archived relation.
        /// </summary>
        internal static Relation GetArchivedRelation(this SqlConnection connection, long id, int version)
        {
            var tagsCommand = connection.GetCommand("select * from relation_tags where relation_id={0} and relation_version={1} order by relation_id",
                id.ToInvariantString(), version.ToInvariantString());
            var membersCommand = connection.GetCommand("select * from relation_members WHERE relation_id={0} and relation_version={1} order by relation_id, sequence_id",
                id.ToInvariantString(), version.ToInvariantString());
            var command = connection.GetCommand("select * from way WHERE id={0} and version={1} order by id",
                id.ToInvariantString(), version.ToInvariantString());

            return new Enumerators.RelationEnumerable(command, tagsCommand, membersCommand).FirstOrDefault();
        }

        /// <summary>
        /// Gets all ways with the given ids.
        /// </summary>
        internal static IEnumerable<Way> GetWaysById(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return Enumerable.Empty<Way>();
            }

            var wayTagsCommand = connection.GetCommand("select * from way_tags WHERE way_id IN ({0}) order by way_id", idsList);
            var wayNodesCommand = connection.GetCommand("select * from way_nodes WHERE way_id IN ({0}) order by way_id, sequence_id", idsList);
            var wayCommand = connection.GetCommand("select * from way WHERE id IN ({0}) order by id", idsList);

            return new Enumerators.WayEnumerable(wayCommand, wayTagsCommand, wayNodesCommand);
        }

        /// <summary>
        /// Gets a changeset for the given id.
        /// </summary>
        internal static Changesets.Changeset GetChangesetById(this SqlConnection connection, long id)
        {
            var tagsCommand = connection.GetCommand("select * from changeset_tags WHERE changeset_id={0}", id.ToInvariantString());
            var command = connection.GetCommand("select * from changeset WHERE id={0} order by id", id.ToInvariantString());

            var reader = command.ExecuteReaderWrapper();
            if (reader.Read())
            {
                var changeset = reader.BuildChangeset();
                var tagsReader = tagsCommand.ExecuteReaderWrapper();
                tagsReader.AddTags(changeset);

                return changeset;
            }
            return null;
        }

        /// <summary>
        /// Gets all relations with the given ids.
        /// </summary>
        internal static IEnumerable<Relation> GetRelationsById(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return Enumerable.Empty<Relation>();
            }

            var tagsCommand = connection.GetCommand("select * from relation_tags WHERE relation_id IN ({0}) order by relation_id", idsList);
            var membersCommand = connection.GetCommand("select * from relation_members WHERE relation_id IN ({0}) order by relation_id, sequence_id", idsList);
            var command = connection.GetCommand("select * from relation WHERE id IN ({0}) order by id", idsList);

            return new Enumerators.RelationEnumerable(command, tagsCommand, membersCommand);
        }

        /// <summary>
        /// Gets the way ids for all ways having nodes in the given ids.
        /// </summary>
        internal static HashSet<long> GetWayIdsForNodes(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            var command = connection.GetCommand("select distinct way_id from way_nodes where node_id in ({0})", idsList);
            var reader = command.ExecuteReaderWrapper();
            var wayIds = new HashSet<long>();
            while (reader.Read())
            {
                wayIds.Add(reader.GetInt64("way_id"));
            }
            return wayIds;
        }

        /// <summary>
        /// Gets the relation ids for all relations having the given members.
        /// </summary>
        internal static HashSet<long> GetRelationIdsForMembers(this SqlConnection connection, IEnumerable<OsmGeoKey> keys)
        {
            var command = connection.GetCommand("select distinct relation_id from relation_members where " +
                "(member_type = 0 and member_id in ({0})) or " +
                "(member_type = 1 and member_id in ({1})) or " +
                "(member_type = 2 and member_id in ({2})) ",
                    keys.Where(x => x.Type == OsmGeoType.Node).Select(x => x.Id).BuildCommaSeperated(),
                    keys.Where(x => x.Type == OsmGeoType.Way).Select(x => x.Id).BuildCommaSeperated(),
                    keys.Where(x => x.Type == OsmGeoType.Relation).Select(x => x.Id).BuildCommaSeperated());

            var reader = command.ExecuteReaderWrapper();
            var relationIds = new HashSet<long>();
            while (reader.Read())
            {
                relationIds.Add(reader.GetInt64("relation_id"));
            }
            return relationIds;
        }

        /// <summary>
        /// Gets the relation ids for all relations having the given members.
        /// </summary>
        internal static HashSet<long> GetRelationIdsForMembers(this SqlConnection connection, IEnumerable<long> nodes, IEnumerable<long> ways)
        {
            SqlCommand command = null;
            var nodeIds = nodes.BuildCommaSeperated();
            var wayIds = ways.BuildCommaSeperated();

            if (!string.IsNullOrWhiteSpace(nodeIds) &&
                !string.IsNullOrWhiteSpace(wayIds))
            {
                command = connection.GetCommand("select distinct relation_id from relation_members where " +
                    "(member_type = 0 and member_id in ({0})) or " +
                    "(member_type = 1 and member_id in ({1})) ",
                        nodeIds, wayIds);
            }
            else if (!string.IsNullOrWhiteSpace(nodeIds))
            {
                command = connection.GetCommand("select distinct relation_id from relation_members where " +
                    "(member_type = 0 and member_id in ({0})) ",
                        nodeIds);
            }
            else if (!string.IsNullOrWhiteSpace(wayIds))
            {
                command = connection.GetCommand("select distinct relation_id from relation_members where " +
                    "(member_type = 1 and member_id in ({0})) ",
                        wayIds);
            }
            else
            {
                return new HashSet<long>();
            }

            var reader = command.ExecuteReaderWrapper();
            var relationIds = new HashSet<long>();
            while (reader.Read())
            {
                relationIds.Add(reader.GetInt64("relation_id"));
            }
            return relationIds;
        }

        /// <summary>
        /// Gets all the nodes in the given bounding box.
        /// </summary>
        internal static HashSet<long> GetNodeIdsInBox(this SqlConnection connection, float minLatitude, float minLongitude, float maxLatitude, float maxLongitude)
        {
            var boxes = new List<long>();
            var tileRange = Tiles.TileRange.CreateAroundBoundingBox(minLatitude, minLongitude, maxLatitude, maxLongitude, 14);
            foreach (var tile in tileRange)
            {
                boxes.Add((long)tile.Id);
            }

            // read all nodes in bounding box.               
            var command = connection.GetCommand(string.Format(
                "select distinct id from node " +
                "left outer join node_tags on node.id = node_tags.node_id " +
                "where (tile in ({0})) AND (latitude >= @minlat AND latitude < @maxlat AND longitude >= @minlon AND longitude < @maxlon) " +
                "order by id",
                    boxes.BuildCommaSeperated()));
            command.Parameters.AddWithValue("minlat", (long)(minLatitude * 10000000));
            command.Parameters.AddWithValue("maxlat", (long)(maxLatitude * 10000000));
            command.Parameters.AddWithValue("minlon", (long)(minLongitude * 10000000));
            command.Parameters.AddWithValue("maxlon", (long)(maxLongitude * 10000000));

            var reader = command.ExecuteReaderWrapper();
            var relationIds = new HashSet<long>();
            while (reader.Read())
            {
                relationIds.Add(reader.GetInt64("id"));
            }
            return relationIds;
        }

        /// <summary>
        /// Archives all the nodes with the given ids.
        /// </summary>
        internal static void ArchiveNodes(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return;
            }

            connection.GetCommand("insert into archived_node_tags select node_id, version, [key], value from node_tags inner join node on node.id = node_tags.node_id where node_id IN ({0}) order by node_id",
                idsList).ExecuteNonQuery();
            connection.GetCommand("insert into archived_node select * from node where id IN ({0}) order by id",
                idsList).ExecuteNonQuery();

            connection.GetCommand("delete from node_tags where node_id in ({0})", idsList).ExecuteNonQuery();
            connection.GetCommand("delete from node where id in ({0})", idsList).ExecuteNonQuery();
        }

        /// <summary>
        /// Archives all the ways with the given ids.
        /// </summary>
        internal static void ArchiveWays(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return;
            }

            connection.GetCommand("insert into archived_way_nodes select way_id, version, node_id, sequence_id from way_nodes inner join way on way.id = way_nodes.way_id where way_id IN ({0}) order by way_id",
                idsList).ExecuteNonQuery();
            connection.GetCommand("insert into archived_way_tags select way_id, version, [key], value from way_tags inner join way on way.id = way_tags.way_id where way_id IN ({0}) order by way_id",
                idsList).ExecuteNonQuery();
            connection.GetCommand("insert into archived_way select * from way where id IN ({0}) order by id",
                idsList).ExecuteNonQuery();

            connection.GetCommand("delete from way_nodes where way_id in ({0})", idsList).ExecuteNonQuery();
            connection.GetCommand("delete from way_tags where way_id in ({0})", idsList).ExecuteNonQuery();
            connection.GetCommand("delete from way where id in ({0})", idsList).ExecuteNonQuery();
        }

        /// <summary>
        /// Archives all the relations with the given ids.
        /// </summary>
        internal static void ArchiveRelations(this SqlConnection connection, IEnumerable<long> ids)
        {
            var idsList = ids.BuildCommaSeperated();

            if (string.IsNullOrWhiteSpace(idsList))
            {
                return;
            }

            connection.GetCommand("insert into archived_relation_members select relation_id, version, member_type, member_id, member_role, sequence_id from relation_members inner join relation on relation.id = relation_members.relation_id where relation_id IN ({0}) order by relation_id",
                idsList).ExecuteNonQuery();
            connection.GetCommand("insert into archived_relation_tags select relation_id, version, [key], value from relation_tags inner join relation on relation.id = relation_tags.relation_id where relation_id IN ({0}) order by relation_id",
                idsList).ExecuteNonQuery();
            connection.GetCommand("insert into archived_relation select * from relation where id IN ({0}) order by id",
                idsList).ExecuteNonQuery();

            connection.GetCommand("delete from relation_members where relation_id in ({0})", idsList).ExecuteNonQuery();
            connection.GetCommand("delete from relation_tags where relation_id in ({0})", idsList).ExecuteNonQuery();
            connection.GetCommand("delete from relation where in id ({0})", idsList).ExecuteNonQuery();
        }
    }
}