/*
NOTES:
    * Don't put 'go' in this script, it will be executed by SqlCommand.ExecuteNonQuery 
    * Please keep SQLServerSimpleSchemaConstants up to date with correct varchar() sizes
*/

if object_id('dbo.node', 'U') is null
  CREATE TABLE dbo.node
  (
    id            bigint   not null,
    latitude      bigint,
    longitude     bigint,
    changeset_id  bigint   null,
    visible       bit      null,
    [timestamp]   bigint null,
    tile          bigint   null,
    [version]     integer  null,
    usr           varchar(100) null,
    usr_id        integer  null,
	PRIMARY KEY CLUSTERED (id)
  ); 

if object_id('dbo.node_tags', 'U') is null
  CREATE TABLE dbo.node_tags
  (
    node_id bigint       not null,
    [key]   varchar(100) not null,
    value   varchar(500) null
  );

CREATE INDEX IDX_NODE_TAGS_NODE ON dbo.node_tags(node_id  ASC);

if object_id('dbo.way', 'U') is null
  CREATE TABLE dbo.way 
  (
    id            bigint   not null,
    changeset_id  bigint   null,
    [timestamp]   bigint null,
    visible       bit      null,
    [version]     integer  null,
    usr           varchar(100) null,
    usr_id        integer  null,
	PRIMARY KEY CLUSTERED (id)
  ); 


if object_id('dbo.way_tags', 'U') is null
  CREATE TABLE dbo.way_tags 
  (
    way_id bigint       not null,
    [key]  varchar(255) not null,
    value  varchar(500) null
  ); 

CREATE INDEX IDX_WAY_TAGS_WAY ON dbo.way_tags(way_id  ASC);

if object_id('dbo.way_nodes', 'U') is null
  CREATE TABLE dbo.way_nodes 
  (
    way_id      bigint  not null,
    node_id     bigint  not null,
    sequence_id integer not null,
	PRIMARY KEY CLUSTERED (way_id, sequence_id)
  ); 
  
CREATE INDEX IDX_WAY_NODES_WAY ON dbo.way_nodes(way_id  ASC);
CREATE INDEX IDX_WAY_NODES_WAY_SEQ ON dbo.way_nodes(way_id, sequence_id ASC);

if object_id('dbo.relation', 'U') is null
  CREATE TABLE dbo.relation 
  (
    id            bigint   not null,
    changeset_id  bigint   null,
    [timestamp]   bigint null,
    visible       bit      null,
    [version]     integer  null,
    usr           varchar(100) null,
    usr_id        integer  null,
	PRIMARY KEY CLUSTERED (id)
  ); 


if object_id('dbo.relation_tags', 'U') is null
  CREATE TABLE dbo.relation_tags 
  (
    relation_id bigint       not null,
    [key]       varchar(100) not null,
    value       varchar(500) null
  ); 

CREATE INDEX IDX_REL_TAGS_REL ON dbo.relation_tags(relation_id  ASC);

if object_id('dbo.relation_members', 'U') is null
  CREATE TABLE dbo.relation_members 
  (
    relation_id bigint       not null,
    member_type int			 null,
    member_id   bigint       not null,
    member_role varchar(100) null,
    sequence_id integer      not null,
	PRIMARY KEY CLUSTERED (relation_id, sequence_id)
  ); 
  
CREATE INDEX IDX_REL_MEM_REL ON dbo.relation_members(relation_id  ASC);
CREATE INDEX IDX_REL_MEM_REL_SEQ ON dbo.relation_members(relation_id, sequence_id ASC);