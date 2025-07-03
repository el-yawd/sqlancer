package sqlancer.turso;

import java.sql.SQLException;
import sqlancer.SQLGlobalState;
import sqlancer.turso.schema.TursoSchema;

public class TursoGlobalState
    extends SQLGlobalState<TursoOptions, TursoSchema> {

    @Override
    protected TursoSchema readSchema() throws SQLException {
        return TursoSchema.fromConnection(this);
    }
}
