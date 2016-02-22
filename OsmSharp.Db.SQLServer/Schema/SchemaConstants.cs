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

namespace OsmSharp.Db.SQLServer.SchemaTools
{
    /// <summary>
    /// string length constants that match the SQL DDL schema.
    /// </summary>
    public static class SchemaConstants
    {
        /// <summary>
        /// dbo.node.usr
        /// </summary>
        public const int NodeUsr = 100;

        /// <summary>
        /// dbo.node_tags.key
        /// </summary>
        public const int NodeTagsKey = 100;

        /// <summary>
        /// dbo.node_tags.value
        /// </summary>
        public const int NodeTagsValue = 500;

        /// <summary>
        /// dbo.way.usr
        /// </summary>
        public const int WayUsr = 100;

        /// <summary>
        /// dbo.way_tags.key
        /// </summary>
        public const int WayTagsKey = 255;

        /// <summary>
        /// dbo.way_tags.value
        /// </summary>
        public const int WayTagsValue = 500;

        /// <summary>
        /// dbo.relation.usr
        /// </summary>
        public const int RelationUsr = 100;

        /// <summary>
        /// dbo.relation_tags.key
        /// </summary>
        public const int RelationTagsKey = 100;

        /// <summary>
        /// dbo.relation_tags.value
        /// </summary>
        public const int RelationTagsValue = 500;

        /// <summary>
        /// dbo.relation_members.member_type
        /// </summary>
        public const int RelationMemberType = 100;

        /// <summary>
        /// dbo.relation_members.member_role
        /// </summary>
        public const int RelationMemberRole = 100;
    }
}