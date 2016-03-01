-- Drop tables in order
IF OBJECT_ID('dbo.node_tags', 'U') IS NOT NULL 
    DROP TABLE dbo.node_tags 
IF OBJECT_ID('dbo.node', 'U') IS NOT NULL 
    DROP TABLE dbo.node
IF OBJECT_ID('dbo.way_nodes', 'U') IS NOT NULL 
    DROP TABLE dbo.way_nodes 
IF OBJECT_ID('dbo.way_tags', 'U') IS NOT NULL 
    DROP TABLE dbo.way_tags 
IF OBJECT_ID('dbo.way', 'U') IS NOT NULL 
    DROP TABLE dbo.way 
IF OBJECT_ID('dbo.relation_tags', 'U') IS NOT NULL 
    DROP TABLE dbo.relation_tags 
IF OBJECT_ID('dbo.relation_members', 'U') IS NOT NULL 
    DROP TABLE dbo.relation_members 
IF OBJECT_ID('dbo.relation', 'U') IS NOT NULL 
    DROP TABLE dbo.relation 

IF OBJECT_ID('dbo.archived_node_tags', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_node_tags 
IF OBJECT_ID('dbo.archived_node', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_node
IF OBJECT_ID('dbo.archived_way_nodes', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_way_nodes 
IF OBJECT_ID('dbo.archived_way_tags', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_way_tags 
IF OBJECT_ID('dbo.archived_way', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_way 
IF OBJECT_ID('dbo.archived_relation_tags', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_relation_tags 
IF OBJECT_ID('dbo.archived_relation_members', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_relation_members 
IF OBJECT_ID('dbo.archived_relation', 'U') IS NOT NULL 
    DROP TABLE dbo.archived_relation 
	
IF OBJECT_ID('dbo.changeset_tags', 'U') IS NOT NULL 
    DROP TABLE dbo.changeset_tags 
IF OBJECT_ID('changeset_changes', 'U') IS NOT NULL
	DROP TABLE dbo.changeset_changes
IF OBJECT_ID('dbo.changeset', 'U') IS NOT NULL 
    DROP TABLE dbo.changeset
