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
    class NodeEnumerable : IEnumerable<Node>
    {
        private readonly SqlCommand _nodeCommand;
        private readonly SqlCommand _nodeTagsCommand;

        public NodeEnumerable(SqlCommand nodeCommand)
        {
            _nodeCommand = nodeCommand;
            _nodeTagsCommand = null;
        }

        public NodeEnumerable(SqlCommand nodeCommand,
            SqlCommand nodeTagsCommand)
        {
            _nodeCommand = nodeCommand;
            _nodeTagsCommand = nodeTagsCommand;
        }

        public IEnumerator<Node> GetEnumerator()
        {
            return new NodeEnumerator(_nodeCommand, _nodeTagsCommand);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new NodeEnumerator(_nodeCommand, _nodeTagsCommand);
        }
    }

    class NodeEnumerator : IEnumerator<Node>
    {
        private readonly SqlCommand _nodeCommand;
        private readonly SqlCommand _nodeTagsCommand;

        public NodeEnumerator(SqlCommand nodeCommand,
            SqlCommand nodeTagsCommand)
        {
            _nodeCommand = nodeCommand;
            _nodeTagsCommand = nodeTagsCommand;
        }

        private Node _current;
        private DbDataReaderWrapper _nodeReader;
        private DbDataReaderWrapper _nodeTagsReader;

        public Node Current
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

        }

        public bool MoveNext()
        {
            if (_nodeReader == null)
            {
                _nodeReader = _nodeCommand.ExecuteReaderWrapper();
                if (_nodeTagsCommand != null)
                {
                    _nodeTagsReader = _nodeTagsCommand.ExecuteReaderWrapper();
                    _nodeTagsReader.Read();
                }
            }
            
            if (!_nodeReader.Read())
            {
                return false;
            }

            if (!_nodeReader.HasActiveRow)
            {
                return false;
            }

            var node = new Node();
            _nodeReader.BuildNode(node);
            if (_nodeTagsReader != null)
            {
                _nodeTagsReader.AddTags(node);
            }
            _current = node;
            return true;
        }

        public void Reset()
        {
            if (_nodeReader != null)
            {
                _nodeReader.Dispose();
            }
            if (_nodeTagsReader != null)
            {
                _nodeTagsReader.Dispose();
            }

            _nodeReader = null;
            _nodeTagsReader = null;
        }
    }
}
