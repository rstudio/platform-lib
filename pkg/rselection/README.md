# rselection

A lightweight leader election library intended for use with `rsnotify`. Provides
an implementation for Postgresql (using pgx). Also provides an implementation
for single-node use where we assume the single node is always the leader. 

Provides interfaces for scheduled and persistent tasks that are run on an
active leader node.
