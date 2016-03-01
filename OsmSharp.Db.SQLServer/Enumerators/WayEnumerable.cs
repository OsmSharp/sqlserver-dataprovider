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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace OsmSharp.Db.SQLServer.Enumerators
{
    class WayEnumerable : IEnumerable<Way>
    {
        private readonly SqlCommand _wayCommand;
        private readonly SqlCommand _wayTagsCommand;
        private readonly SqlCommand _wayNodesCommand;

        public WayEnumerable(SqlCommand wayCommand,
            SqlCommand wayTagsCommand, SqlCommand wayNodesCommand)
        {
            _wayCommand = wayCommand;
            _wayTagsCommand = wayTagsCommand;
            _wayNodesCommand = wayNodesCommand;
        }

        public IEnumerator<Way> GetEnumerator()
        {
            return new WayEnumerator(_wayCommand, _wayTagsCommand, _wayNodesCommand);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new WayEnumerator(_wayCommand, _wayTagsCommand, _wayNodesCommand);
        }
    }

    class WayEnumerator : IEnumerator<Way>
    {
        private readonly SqlCommand _wayCommand;
        private readonly SqlCommand _wayTagsCommand;
        private readonly SqlCommand _wayNodesCommand;

        public WayEnumerator(SqlCommand wayCommand,
            SqlCommand wayTagsCommand, SqlCommand wayNodesCommand)
        {
            _wayCommand = wayCommand;
            _wayTagsCommand = wayTagsCommand;
            _wayNodesCommand = wayNodesCommand;
        }

        private Way _current;
        private DbDataReaderWrapper _wayReader;
        private DbDataReaderWrapper _wayTagsReader;
        private DbDataReaderWrapper _wayNodesReader;

        public Way Current
        {
            get
            {
                if (_current == null)
                {
                    throw new InvalidOperationException("No data available.");
                }
                return _current;
            }
        }

        object IEnumerator.Current
        {
            get
            {
                return this.Current;
            }
        }

        public void Dispose()
        {
            if (_wayReader != null)
            {
                _wayReader.Dispose();
            }
            if (_wayTagsReader != null)
            {
                _wayTagsReader.Dispose();
            }
            if (_wayNodesReader != null)
            {
                _wayNodesReader.Dispose();
            }
        }

        public bool MoveNext()
        {
            if (_wayReader == null)
            {
                _wayReader = _wayCommand.ExecuteReaderWrapper();
                if (_wayTagsCommand != null)
                {
                    _wayTagsReader = _wayTagsCommand.ExecuteReaderWrapper();
                    _wayTagsReader.Read();
                }
                if (_wayNodesCommand != null)
                {
                    _wayNodesReader = _wayNodesCommand.ExecuteReaderWrapper();
                    _wayNodesReader.Read();
                }
            }

            if (!_wayReader.Read())
            {
                return false;
            }

            if (!_wayReader.HasActiveRow)
            {
                return false;
            }

            var way = new Way();
            _wayReader.BuildWay(way);
            if (_wayTagsReader != null)
            {
                _wayTagsReader.AddTags(way);
            }
            if (_wayNodesReader != null)
            {
                _wayNodesReader.AddNodes(way);
            }
            _current = way;
            return true;
        }

        public void Reset()
        {
            if (_wayReader != null)
            {
                _wayReader.Dispose();
            }
            if (_wayTagsReader != null)
            {
                _wayTagsReader.Dispose();
            }
            if (_wayNodesReader != null)
            {
                _wayNodesReader.Dispose();
            }

            _wayReader = null;
            _wayTagsReader = null;
            _wayNodesReader = null;
        }
    }
}