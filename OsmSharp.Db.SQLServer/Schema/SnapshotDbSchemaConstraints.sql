-- Create indexes
IF NOT EXISTS (SELECT  1 FROM sysindexes WHERE name = 'IDX_NODE_TILE' AND id = OBJECT_ID('dbo.node'))
    CREATE INDEX IDX_NODE_TILE ON node(tile  ASC) ;

IF NOT EXISTS (SELECT 1 FROM sysindexes WHERE name = 'IDX_WAY_NODES_NODE' AND id = OBJECT_ID('dbo.way_nodes'))
    CREATE INDEX IDX_WAY_NODES_NODE ON dbo.way_nodes(node_id  ASC) ;

--IF NOT EXISTS (SELECT 1 FROM  sysindexes WHERE name = 'IDX_WAY_NODES_WAY_SEQUENCE' AND id = OBJECT_ID('dbo.way_nodes'))
--    CREATE INDEX IDX_WAY_NODES_WAY_SEQUENCE ON dbo.way_nodes(way_id  ASC,sequence_id  ASC) ;

-- Create foreign keys
ALTER TABLE dbo.node_tags ADD CONSTRAINT FK_node_tags_node FOREIGN KEY (node_id) REFERENCES dbo.node (id)
ALTER TABLE dbo.way_tags ADD CONSTRAINT FK_way_tags_way FOREIGN KEY (way_id) REFERENCES dbo.way (id)
ALTER TABLE dbo.way_nodes ADD CONSTRAINT FK_way_nodes_way FOREIGN KEY (way_id) REFERENCES dbo.way (id)
-- ALTER TABLE dbo.way_nodes ADD CONSTRAINT FK_way_nodes_node FOREIGN KEY (node_id) REFERENCES dbo.node (id) (not all nodes will be there in some circumstances)
ALTER TABLE dbo.relation_members ADD CONSTRAINT FK_relation_members_relation FOREIGN KEY (relation_id) REFERENCES dbo.relation (id)
ALTER TABLE dbo.relation_tags ADD CONSTRAINT FK_relation_tags_relation FOREIGN KEY (relation_id) REFERENCES dbo.relation (id)