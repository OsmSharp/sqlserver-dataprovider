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
using System;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer.Streams
{
    /// <summary>
    /// An osm stream source that reads all data from the database. 
    /// </summary>
    public class SnapshotDbStreamSource : OsmStreamSource
    {
        private readonly string _connectionString;

        /// <summary>
        /// Creates a new snapshot db.
        /// </summary>
        public SnapshotDbStreamSource(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Creates a new snapshot db.
        /// </summary>
        public SnapshotDbStreamSource(SqlConnection connection)
        {
            _connection = connection;
        }

        private SqlConnection _connection; // Holds the connection to the SQLServer db.

        private DbDataReaderWrapper _nodeReader;
        private DbDataReaderWrapper _nodeTagsReader;
        private DbDataReaderWrapper _wayReader;
        private DbDataReaderWrapper _wayTagsReader;
        private DbDataReaderWrapper _wayNodesReader;
        private DbDataReaderWrapper _relationReader;
        private DbDataReaderWrapper _relationMembersReader;
        private DbDataReaderWrapper _relationTagsReader;

        private OsmGeoType? _currentType;

        /// <summary>
        /// Gets the connection.
        /// </summary>
        private SqlConnection GetConnection()
        {
            if (_connection == null)
            {
                _connection = new SqlConnection(_connectionString);
                _connection.Open();
            }
            return _connection;
        }

        /// <summary>
        /// Gets the sql command.
        /// </summary>
        private SqlCommand GetCommand(string sql)
        {
            return new SqlCommand(sql, this.GetConnection());
        }

        /// <summary>
        /// Returns true if this source can be reset.
        /// </summary>
        public override bool CanReset
        {
            get
            {
                return true;
            }
        }

        private bool _initialized = false;

        /// <summary>
        /// Initializes this source.
        /// </summary>
        private void Initialize()
        {
            _initialized = true;
            var command = this.GetCommand("SELECT id, latitude, longitude, changeset_id, visible, timestamp, tile, [version], usr, usr_id " +
                "FROM dbo.node " +
                "ORDER BY id");
            _nodeReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT node_id, [key], value " +
                "FROM dbo.node_tags " +
                "ORDER BY node_id");
            _nodeTagsReader = new DbDataReaderWrapper(command.ExecuteReader());

            command = this.GetCommand("SELECT id, changeset_id, visible, timestamp, [version], usr, usr_id " +
                "FROM dbo.way " +
                "ORDER BY id");
            _wayReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT way_id, [key], value " +
                "FROM dbo.way_tags " +
                "ORDER BY way_id");
            _wayTagsReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT way_id, node_id, sequence_id  " +
                "FROM dbo.way_nodes " +
                "ORDER BY way_id, sequence_id");
            _wayNodesReader = new DbDataReaderWrapper(command.ExecuteReader());

            command = this.GetCommand("SELECT id, changeset_id, visible, timestamp, [version], usr, usr_id " +
                "FROM dbo.relation " +
                "ORDER BY id");
            _relationReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT relation_id, [key], value " +
                "FROM dbo.relation_tags " +
                "ORDER BY relation_id");
            _relationTagsReader = new DbDataReaderWrapper(command.ExecuteReader());
            command = this.GetCommand("SELECT relation_id, member_type, member_role, member_id, sequence_id " +
                "FROM dbo.relation_members " +
                "ORDER BY relation_id, sequence_id");
            _relationMembersReader = new DbDataReaderWrapper(command.ExecuteReader());
        }

        /// <summary>
        /// Returns the current object.
        /// </summary>
        /// <returns></returns>
        public override OsmGeo Current()
        {
            if(_currentType == null)
            {
                throw new Exception("No current object available.");
            }

            if(_currentType.Value == OsmGeoType.Node)
            {
                var node = _nodeReader.BuildNode();
                _nodeTagsReader.AddTags(node);
                return node;
            }
            else if (_currentType.Value == OsmGeoType.Way)
            {
                var way = _wayReader.BuildWay();
                _wayTagsReader.AddTags(way);
                _wayNodesReader.AddNodes(way);
                return way;
            }
            else if (_currentType.Value == OsmGeoType.Relation)
            {
                var relation = _relationReader.BuildRelation();
                _relationTagsReader.AddTags(relation);
                _relationMembersReader.AddMembers(relation);
                return relation;
            }
            throw new Exception("No current object available.");
        }

        /// <summary>
        /// Move to the next object.
        /// </summary>
        public override bool MoveNext(bool ignoreNodes, bool ignoreWays, bool ignoreRelations)
        {
            if (!_initialized)
            {
                this.Initialize();
            }

            if (_currentType == null)
            { // first move.
                _currentType = OsmGeoType.Node;
                _nodeTagsReader.Read();
            }

            if(_currentType.Value == OsmGeoType.Node)
            {
                if(ignoreNodes)
                { // ignore nodes, move to way..
                    _currentType = OsmGeoType.Way;
                    _wayNodesReader.Read();
                    _wayTagsReader.Read();
                }
                else
                { // nodes not be be ignored, move to next node.
                    if(_nodeReader.Read())
                    { // move succeeded.
                        return true;
                    }
                    else
                    { // move not succeeded, move to ways.
                        _currentType = OsmGeoType.Way;
                        _wayNodesReader.Read();
                        _wayTagsReader.Read();
                    }
                }
            }
            if (_currentType.Value == OsmGeoType.Way)
            {
                if (ignoreWays)
                { // ignore ways, move to relations..
                    _currentType = OsmGeoType.Relation;
                    _relationMembersReader.Read();
                    _relationTagsReader.Read();
                }
                else
                { // ways not be ignored, move to next way.
                    if(_wayReader.Read())
                    { // move succeeded.
                        return true;
                    }
                    else
                    { // move no success, move to relations.
                        _currentType = OsmGeoType.Relation;
                        _relationMembersReader.Read();
                        _relationTagsReader.Read();
                    }
                }
            }
            if (_currentType.Value == OsmGeoType.Relation)
            {
                if(ignoreRelations)
                { // ignore relations.
                    return false;
                }
                else
                { // don't ignore relations, try to read.
                    if(_relationReader.Read())
                    {
                        return true;
                    }
                    else
                    { // move no success.
                        return false;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Resets this source.
        /// </summary>
        public override void Reset()
        {
            if (_nodeReader != null) { (_nodeReader as IDisposable).Dispose(); }
            if (_nodeTagsReader != null) { (_nodeTagsReader as IDisposable).Dispose(); }
            if (_wayReader != null) { (_wayReader as IDisposable).Dispose(); }
            if (_wayTagsReader != null) { (_wayTagsReader as IDisposable).Dispose(); }
            if (_wayNodesReader != null) { (_wayNodesReader as IDisposable).Dispose(); }
            if (_relationReader != null) { (_relationReader as IDisposable).Dispose(); }
            if (_relationMembersReader != null) { (_relationMembersReader as IDisposable).Dispose(); }
            if (_relationTagsReader != null) { (_relationTagsReader as IDisposable).Dispose(); }

            this.Initialize();
        }
    }
}