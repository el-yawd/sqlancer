package sqlancer.limbo;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;
import sqlancer.limbo.schema.LimboSchema;

public class LimboGlobalState extends SQLGlobalState<LimboOptions, LimboSchema> {

    @Override
    protected LimboSchema readSchema() throws SQLException {
        return LimboSchema.fromConnection(this);
    }

}
