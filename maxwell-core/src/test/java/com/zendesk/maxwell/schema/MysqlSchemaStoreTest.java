package com.zendesk.maxwell.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import com.zendesk.maxwell.*;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Test;

public class MysqlSchemaStoreTest extends MaxwellTestWithIsolatedServer {
    @Test
    public void testGetSchemaID() throws Exception {
        BinlogPosition bl_pos = new BinlogPosition(0, "mysql.1234");
        Position pos = new Position(bl_pos, 1);
        MysqlSchemaStore schemaStore = new MysqlSchemaStore(buildContext(), pos);
        assertThat(schemaStore.getSchemaID(), is(1L));

        String sql = "CREATE DATABASE `testdb`;";
        String db = "testdb";
        BinlogPosition bl_pos2 = new BinlogPosition(1, "mysql.1234");
        Position pos2 = new Position(bl_pos2, 1);
        schemaStore.processSQL(sql, db, pos2);
        assertThat(schemaStore.getSchemaID(), is(2L));
    }
}
