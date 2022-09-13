//! Provides a DataFusion-powered implementation of the [Engine] trait.

use std::sync::Arc;

use async_trait::async_trait;
use convergence::engine::{Engine, Portal};
use convergence::protocol::{DataTypeOid, ErrorResponse, FieldDescription, Severity, SqlState};
use convergence::protocol_ext::DataRowBatch;
use convergence::server::{self, BindOptions};
use rusqlite::Connection;
use sqlparser::ast::Statement;
use tokio::sync::Mutex;

struct SqlitePortal {
    statement: Statement,
    connection: Arc<Mutex<Connection>>,
}

#[async_trait]
impl Portal for SqlitePortal {
    async fn fetch(&mut self, batch: &mut DataRowBatch) -> Result<(), ErrorResponse> {
        let connection = self.connection.lock().await;

        let sql = self.statement.to_string();
        let sql = sql.as_str();

        let prepared_statement = connection.prepare(sql);

        if let Err(err) = prepared_statement {
            return Err(ErrorResponse {
                sql_state: SqlState::SYNTAX_ERROR,
                severity: Severity::ERROR,
                message: err.to_string(),
            });
        }

        let prepared_statement = prepared_statement.unwrap();

        let columns = prepared_statement.columns();

        match self.statement {
            Statement::Query(_) => {
                if let Err(err) = connection.query_row(sql, [], |row| {
                    let mut row_writer = batch.create_row();

                    for column in columns {
                        let column_name = column.name();
                        let column_index = prepared_statement.column_index(column_name).unwrap();

                        let decl_type = column.decl_type().unwrap();
                        let decl_type = decl_type.to_lowercase();
                        let decl_type = decl_type.as_str();

                        match decl_type {
                            "text" => {
                                let val: String = row.get(column_index).unwrap();
                                row_writer.write_string(&val);
                            }
                            _ => todo!(),
                        }
                    }

                    Ok(())
                }) {
                    return Err(ErrorResponse {
                        sql_state: SqlState::DATA_EXCEPTION,
                        severity: Severity::ERROR,
                        message: err.to_string(),
                    });
                }
            }
            _ => {
                let execution_result = connection.execute(sql, []);

                if let Err(err) = execution_result {
                    return Err(ErrorResponse {
                        sql_state: SqlState::DATA_EXCEPTION,
                        severity: Severity::ERROR,
                        message: err.to_string(),
                    });
                }

                let changed = execution_result.unwrap();

                batch.create_row().write_int8(changed as i64);
            }
        }

        Ok(())
    }
}

/// An engine instance using DataFusion for catalogue management and queries.
struct SqliteEngine {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteEngine {
    /// Creates a new engine instance using the given DataFusion execution context.
    pub async fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }
}

#[async_trait]
impl Engine for SqliteEngine {
    type PortalType = SqlitePortal;

    async fn prepare(
        &mut self,
        statement: &Statement,
    ) -> Result<Vec<FieldDescription>, ErrorResponse> {
        let connection = self.connection.lock().await;

        let sql = statement.to_string();
        let sql = sql.as_str();

        let prepared_statement = connection.prepare(sql);

        if let Err(err) = prepared_statement {
            return Err(ErrorResponse {
                sql_state: SqlState::SYNTAX_ERROR,
                severity: Severity::ERROR,
                message: err.to_string(),
            });
        }

        let prepared_statement = prepared_statement.unwrap();

        let mut field_descriptions = Vec::new();

        match statement {
            Statement::Query(_) => {
                let columns = prepared_statement.columns();

                for column in columns {
                    let column_name = column.name();

                    let decl_type = column.decl_type().unwrap();
                    let decl_type = decl_type.to_lowercase();
                    let decl_type = decl_type.as_str();

                    let data_type_oid = match decl_type {
                        "text" => DataTypeOid::Text,
                        _ => todo!(),
                    };

                    field_descriptions.push(FieldDescription {
                        data_type: data_type_oid,
                        name: column_name.to_string(),
                    });
                }
            }
            Statement::Insert { .. } | Statement::CreateTable { .. } => {
                field_descriptions.push(FieldDescription {
                    name: "changed".to_string(),
                    data_type: DataTypeOid::Int8,
                });
            }
            _ => todo!(),
        }

        Ok(field_descriptions)
    }

    async fn create_portal(
        &mut self,
        statement: &Statement,
    ) -> Result<Self::PortalType, ErrorResponse> {
        Ok(SqlitePortal {
            statement: statement.clone(),
            connection: self.connection.clone(),
        })
    }
}

#[tokio::main]
async fn main() {
    server::run(
        BindOptions::new().with_addr("127.0.0.1").with_port(5432),
        Arc::new(|| {
            Box::pin({
                let connection = Connection::open_in_memory().unwrap();

                connection
                    .execute("CREATE TABLE foobar (id TEXT PRIMARY KEY)", [])
                    .unwrap();
                connection
                    .execute("INSERT INTO foobar (id) VALUES ('john'), ('doe')", [])
                    .unwrap();

                SqliteEngine::new(Arc::new(Mutex::new(connection)))
            })
        }),
    )
    .await
    .unwrap();
}
