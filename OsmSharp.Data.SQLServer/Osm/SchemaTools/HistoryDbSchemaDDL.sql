-- this script builds a full history db.

if object_id('dbo.node', 'U') is null
  CREATE TABLE dbo.node
  (
    id            bigint   not null,
    latitude      integer  not null,
    longitude     integer  not null,
    changeset_id  bigint   not null,
    visible       bit      not null,
    [timestamp]   datetime not null,
    tile          bigint   not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null
  ); 



if object_id('dbo.node_tags', 'U') is null
  CREATE TABLE dbo.node_tags
  (
    node_id bigint       not null,
	[version] integer    not null,
    [key]   varchar(100) not null,
    value   varchar(500) null
  );


if object_id('dbo.way', 'U') is null
  CREATE TABLE dbo.way 
  (
    id            bigint   not null,
    changeset_id  bigint   not null,
    [timestamp]   datetime not null,
    visible       bit      not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null
  ); 


if object_id('dbo.way_tags', 'U') is null
  CREATE TABLE dbo.way_tags 
  (
    way_id bigint       not null,
	[version] integer   not null,
    [key]  varchar(255) not null,
    value  varchar(500) null
  ); 


if object_id('dbo.way_nodes', 'U') is null
  CREATE TABLE dbo.way_nodes 
  (
    way_id      bigint  not null,
	[version]   integer not null,
    node_id     bigint  not null,
    sequence_id integer not null
  ); 


if object_id('dbo.relation', 'U') is null
  CREATE TABLE dbo.relation 
  (
    id            bigint   not null,
    changeset_id  bigint   not null,
    [timestamp]   datetime not null,
    visible       bit      not null,
    [version]     integer  not null,
    usr           varchar(100) not null,
    usr_id        integer  not null
  );


if object_id('dbo.relation_tags', 'U') is null
  CREATE TABLE dbo.relation_tags 
  (
    relation_id bigint       not null,
	[version]   integer      not null,
    [key]       varchar(100) not null,
    value       varchar(500) null
  ); 


if object_id('dbo.relation_members', 'U') is null
  CREATE TABLE dbo.relation_members 
  (
    relation_id bigint       not null,
	[version]   integer      not null,
    member_type int			 null,
    member_id   bigint       not null,
    member_role varchar(100) null,
    sequence_id integer      not null
  );

if object_id('dbo.changesets', 'U') is null
  CREATE TABLE dbo.changesets 
  (
    id			bigint       not null,
	[usr_id]	integer		 not null,	
    created_at  datetime     not null,
    min_lat     integer      not null,
    max_lat     integer      not null,
    min_lon     integer      not null,
    max_lon     integer      not null,
    closed_at   datetime      not null
  );

if object_id('dbo.changeset_tags', 'U') is null
  CREATE TABLE dbo.changeset_tags 
  (
    changeset_id bigint       not null,
    [key]        varchar(100) not null,
    value        varchar(500) null
  );