-- this script builds a full history db.

if object_id('dbo.node', 'U') is null
  CREATE TABLE dbo.node
  (
    id            bigint   not null,
    latitude      bigint  not null,
    longitude     bigint  not null,
    changeset_id  bigint   not null,
    visible       bit      not null,
    [timestamp]   bigint not null,
    tile          bigint   not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null,
	PRIMARY KEY CLUSTERED (id)
  ); 

CREATE INDEX IDX_NODE_TILE ON dbo.node(tile ASC);

if object_id('dbo.node_tags', 'U') is null
  CREATE TABLE dbo.node_tags
  (
    node_id bigint       not null,
    [key]   varchar(100) not null,
    value   varchar(500) null
  );

CREATE INDEX IDX_NODE_TAGS_NODE ON dbo.node_tags(node_id ASC);

if object_id('dbo.way', 'U') is null
  CREATE TABLE dbo.way 
  (
    id            bigint   not null,
    changeset_id  bigint   not null,
    [timestamp]   bigint not null,
    visible       bit      not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null,
	PRIMARY KEY CLUSTERED (id)
  ); 

if object_id('dbo.way_tags', 'U') is null
  CREATE TABLE dbo.way_tags 
  (
    way_id bigint       not null,
    [key]  varchar(255) not null,
    value  varchar(500) null
  ); 

CREATE INDEX IDX_WAY_TAGS_WAY ON dbo.way_tags(way_id ASC);

if object_id('dbo.way_nodes', 'U') is null
  CREATE TABLE dbo.way_nodes 
  (
    way_id      bigint  not null,
    node_id     bigint  not null,
    sequence_id integer not null,
	PRIMARY KEY CLUSTERED (way_id, sequence_id)
  ); 
  
CREATE INDEX IDX_WAY_NODES_NODE ON dbo.way_nodes(node_id  ASC);
CREATE INDEX IDX_WAY_NODES_WAY ON dbo.way_nodes(way_id ASC);
CREATE INDEX IDX_WAY_NODES_WAY_SEQ ON dbo.way_nodes(way_id, sequence_id ASC);

if object_id('dbo.relation', 'U') is null
  CREATE TABLE dbo.relation 
  (
    id            bigint   not null,
    changeset_id  bigint   not null,
    [timestamp]   bigint not null,
    visible       bit      not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null,
	PRIMARY KEY CLUSTERED (id)
  );
  
if object_id('dbo.relation_tags', 'U') is null
  CREATE TABLE dbo.relation_tags 
  (
    relation_id bigint       not null,
    [key]       varchar(100) not null,
    value       varchar(500) null
  ); 

CREATE INDEX IDX_REL_TAGS_REL ON dbo.relation_tags(relation_id ASC);

if object_id('dbo.relation_members', 'U') is null
  CREATE TABLE dbo.relation_members 
  (
    relation_id bigint       not null,
    member_type int			 null,
    member_id   bigint       not null,
    member_role varchar(100) null,
    sequence_id integer      not null
  );
  
CREATE INDEX IDX_REL_MEM_REL ON dbo.relation_members(relation_id ASC);
CREATE INDEX IDX_REL_MEM_MEM_TYPE ON dbo.relation_members(member_id, member_type  ASC);
CREATE INDEX IDX_REL_MEM_REL_SEQ ON dbo.relation_members(relation_id, sequence_id ASC);

if object_id('dbo.changeset', 'U') is null
  CREATE TABLE dbo.changeset
  (
    id			bigint       not null,
	[usr_id]	integer		 not null,	
    created_at  bigint       not null,
    min_lat     integer      not null,
    max_lat     integer      not null,
    min_lon     integer      not null,
    max_lon     integer      not null,
    closed_at   bigint       null,
	PRIMARY KEY CLUSTERED (id)
  );

if object_id('dbo.changeset_tags', 'U') is null
  CREATE TABLE dbo.changeset_tags 
  (
    changeset_id bigint       not null,
    [key]        varchar(100) not null,
    value        varchar(500) null
  );

if object_id('dbo.changeset_changes', 'U') is null
  CREATE TABLE dbo.changeset_changes 
  (
	[changeset_id] integer NOT NULL,
	[type] integer NOT NULL,
	[osm_id] bigint NOT NULL,
	[osm_type] integer NOT NULL,
	[osm_version] integer NOT NULL
  );

  if object_id('dbo.archived_node', 'U') is null
  CREATE TABLE dbo.archived_node
  (
    id            bigint   not null,
    latitude      bigint  not null,
    longitude     bigint  not null,
    changeset_id  bigint   not null,
    visible       bit      not null,
    [timestamp]   bigint not null,
    tile          bigint   not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null,
	PRIMARY KEY CLUSTERED (id, [version])
  ); 

if object_id('dbo.archived_node_tags', 'U') is null
  CREATE TABLE dbo.archived_node_tags
  (
    node_id bigint       not null,
	[node_version] integer    not null,
    [key]   varchar(100) not null,
    value   varchar(500) null
  );

if object_id('dbo.archived_way', 'U') is null
  CREATE TABLE dbo.archived_way 
  (
    id            bigint   not null,
    changeset_id  bigint   not null,
    [timestamp]   bigint not null,
    visible       bit      not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null,
	PRIMARY KEY CLUSTERED (id, [version])
  ); 

if object_id('dbo.archived_way_tags', 'U') is null
  CREATE TABLE dbo.archived_way_tags 
  (
    way_id bigint       not null,
	[way_version] integer   not null,
    [key]  varchar(255) not null,
    value  varchar(500) null
  ); 

if object_id('dbo.archived_way_nodes', 'U') is null
  CREATE TABLE dbo.archived_way_nodes 
  (
    way_id      bigint  not null,
	[way_version]   integer not null,
    node_id     bigint  not null,
    sequence_id integer not null,
	PRIMARY KEY CLUSTERED (way_id, [way_version], sequence_id)
  ); 

if object_id('dbo.archived_relation', 'U') is null
  CREATE TABLE dbo.archived_relation 
  (
    id            bigint   not null,
    changeset_id  bigint   not null,
    [timestamp]   bigint not null,
    visible       bit      not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null,
	PRIMARY KEY CLUSTERED (id, [version])
  );


if object_id('dbo.archived_relation_tags', 'U') is null
  CREATE TABLE dbo.archived_relation_tags 
  (
    relation_id bigint       not null,
	[relation_version]   integer      not null,
    [key]       varchar(100) not null,
    value       varchar(500) null
  ); 

if object_id('dbo.archived_relation_members', 'U') is null
  CREATE TABLE dbo.archived_relation_members 
  (
    relation_id bigint       not null,
	[relation_version]   integer      not null,
    member_type int			 null,
    member_id   bigint       not null,
    member_role varchar(100) null,
    sequence_id integer      not null
  );
  