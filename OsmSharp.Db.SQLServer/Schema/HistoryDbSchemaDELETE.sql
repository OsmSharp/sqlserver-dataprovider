﻿-- Use this script to delete all information from the database
TRUNCATE TABLE way_tags;
TRUNCATE TABLE way_nodes;
TRUNCATE TABLE way;
TRUNCATE TABLE node_tags;
TRUNCATE TABLE node;
TRUNCATE TABLE relation_members;
TRUNCATE TABLE relation_tags;
TRUNCATE TABLE relation;

TRUNCATE TABLE archived_way_tags;
TRUNCATE TABLE archived_way_nodes;
TRUNCATE TABLE archived_way;
TRUNCATE TABLE archived_node_tags;
TRUNCATE TABLE archived_node;
TRUNCATE TABLE archived_relation_members;
TRUNCATE TABLE archived_relation_tags;
TRUNCATE TABLE archived_relation;

TRUNCATE TABLE changeset_changes;
TRUNCATE TABLE changeset_tags;
TRUNCATE TABLE changeset;