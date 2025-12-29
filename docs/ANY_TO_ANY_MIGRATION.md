# Any-to-Any Database Migration

## Overview
The ETL system now supports full bidirectional database migration between all supported database types:
- SQLite
- Microsoft SQL Server
- Azure SQL
- PostgreSQL
- MySQL

## Features

### Source Database Support
Any of the supported database types can be used as a migration source:
- **SQLite**: Read from local .db file
- **MS SQL/Azure SQL**: Read from SQL Server instance
- **PostgreSQL**: Read from PostgreSQL server
- **MySQL**: Read from MySQL/MariaDB server

### Destination Database Support
Any of the supported database types can be used as a migration destination:
- **SQLite**: Write to local .db file
- **MS SQL/Azure SQL**: Write to SQL Server instance
- **PostgreSQL**: Write to PostgreSQL server
- **MySQL**: Write to MySQL/MariaDB server

### Migration Capabilities

#### Schema Creation
- Automatically creates tables, indexes, and views in the destination database
- Uses the canonical SQLite schema as a template
- Converts schema syntax to match destination database type:
  - SQLite → MS SQL: Converts data types, removes IF NOT EXISTS, converts view syntax
  - SQLite → PostgreSQL: Converts data types, uses IF NOT EXISTS syntax
  - SQLite → MySQL: Converts data types, uses backtick quoting

#### Data Migration
- Reads all data from source database tables
- Inserts data into destination database with proper placeholder syntax:
  - SQLite/MS SQL: Uses `?` placeholders
  - PostgreSQL/MySQL: Uses `%s` placeholders
- Handles different row types (tuples, Row objects, dictionaries)
- Commits data after each table migration
- Tracks success/failure status per table

#### Connection Validation
- Tests both source and destination connections before migration
- Provides clear error messages if connections fail
- Verifies credentials and network connectivity

#### Safety Features
- Only allows migration to empty databases (prevents data corruption)
- Checks if destination has existing data before starting
- Per-table error handling (continues with other tables if one fails)
- Comprehensive logging to server activity log

## Usage

### Via Web UI

1. **Open Admin Control Panel** → Database tab
2. **Click "Migrate Database"** button
3. **Configure Source Database**:
   - Select database type from dropdown
   - Fill in connection details (host, database, credentials)
   - Click "Test Source Connection" to verify
4. **Configure Destination Database**:
   - Select database type from dropdown
   - Fill in connection details
   - Click "Test Destination Connection" to verify
5. **Start Migration**:
   - Click "Start Migration" button
   - Confirm the migration action
   - Monitor progress in the Server Activity Log

### Migration Combinations

All 20 possible combinations are supported:

| Source | Destination | Status |
|--------|-------------|--------|
| SQLite | MS SQL | ✅ Supported |
| SQLite | Azure SQL | ✅ Supported |
| SQLite | PostgreSQL | ✅ Supported |
| SQLite | MySQL | ✅ Supported |
| MS SQL | SQLite | ✅ Supported |
| MS SQL | Azure SQL | ✅ Supported |
| MS SQL | PostgreSQL | ✅ Supported |
| MS SQL | MySQL | ✅ Supported |
| Azure SQL | SQLite | ✅ Supported |
| Azure SQL | MS SQL | ✅ Supported |
| Azure SQL | PostgreSQL | ✅ Supported |
| Azure SQL | MySQL | ✅ Supported |
| PostgreSQL | SQLite | ✅ Supported |
| PostgreSQL | MS SQL | ✅ Supported |
| PostgreSQL | Azure SQL | ✅ Supported |
| PostgreSQL | MySQL | ✅ Supported |
| MySQL | SQLite | ✅ Supported |
| MySQL | MS SQL | ✅ Supported |
| MySQL | Azure SQL | ✅ Supported |
| MySQL | PostgreSQL | ✅ Supported |

## Implementation Details

### Backend Endpoint
**Route**: `POST /api/admincp/database/migrate-data`

**Parameters**:
- `source_db_type`: Source database type (sqlite/mssql/postgresql/mysql)
- `source_sqlite_path`: Path for SQLite source (if applicable)
- `source_mssql_*`: MS SQL connection details (if applicable)
- `source_postgresql_*`: PostgreSQL connection details (if applicable)
- `source_mysql_*`: MySQL connection details (if applicable)
- `destination_db_type`: Destination database type
- `destination_sqlite_path`: Path for SQLite destination (if applicable)
- `destination_mssql_*`: MS SQL connection details (if applicable)
- `destination_postgresql_*`: PostgreSQL connection details (if applicable)
- `destination_mysql_*`: MySQL connection details (if applicable)
- `create_tables`: Whether to create tables (default: true)

**Returns**:
```json
{
  "success": true/false,
  "message": "Migration summary message",
  "total_records": 12345,
  "migration_results": {
    "people": {"records": 100, "status": "success", "message": "Migrated 100 records"},
    "cases": {"records": 200, "status": "success", "message": "Migrated 200 records"}
  },
  "source_type": "mssql",
  "destination_type": "postgresql"
}
```

### Schema Conversion
The system uses the canonical SQLite schema as a base template and converts it to the destination format:
- `convert_sqlite_to_mssql()`: Converts to MS SQL syntax
- `convert_sqlite_to_postgresql()`: Converts to PostgreSQL syntax
- `convert_sqlite_to_mysql()`: Converts to MySQL syntax

This approach ensures schema consistency across all database types.

### Tables Migrated
1. `etl_metadata` - ETL run metadata
2. `people` - Person records
3. `employees` - Employee records
4. `cases` - Case management records
5. `referrals` - Referral records
6. `assistance_requests` - Assistance request records
7. `assistance_requests_supplemental_responses` - Supplemental responses
8. `resource_lists` - Resource list records
9. `resource_list_shares` - Resource list sharing records
10. `data_quality_issues` - Data quality tracking

## Limitations

1. **Empty Destination Required**: Migration only works to empty databases to prevent accidental data loss
2. **Schema Template**: Uses SQLite schema as canonical template (not source schema introspection)
3. **Data Types**: Some data type conversions may not be perfect (e.g., BLOB handling varies)
4. **Large Datasets**: Very large tables may take significant time to migrate
5. **Referential Integrity**: Foreign key constraints must be handled by the schema converters

## Error Handling

- Connection failures are reported immediately
- Per-table errors don't stop the entire migration
- All errors are logged to the server activity log
- Failed tables are listed in the migration results
- Detailed error messages help troubleshooting

## Best Practices

1. **Test Connections First**: Always test source and destination connections before starting migration
2. **Backup Data**: Create backups before migrating from production databases
3. **Empty Destination**: Ensure destination database is empty or use a new database
4. **Network Connectivity**: Verify network access between application and database servers
5. **Credentials**: Use accounts with appropriate read (source) and write (destination) permissions
6. **Monitor Logs**: Watch the server activity log during migration for any issues
7. **Validate Results**: After migration, verify record counts match expected values

## Example Scenarios

### Migrating from SQLite to MS SQL
**Use Case**: Moving from development (SQLite) to production (MS SQL)
1. Configure source as SQLite with path to .db file
2. Configure destination as MS SQL with server credentials
3. Test both connections
4. Start migration
5. Verify record counts match

### Migrating from MS SQL to PostgreSQL
**Use Case**: Switching cloud providers or database engines
1. Configure source as MS SQL with connection details
2. Configure destination as PostgreSQL with connection details
3. Test both connections
4. Start migration
5. Update application database configuration to point to PostgreSQL

### Migrating from Azure SQL to MySQL
**Use Case**: Cost optimization or vendor change
1. Configure source as Azure SQL (same as MS SQL) with connection details
2. Configure destination as MySQL with connection details
3. Test both connections
4. Start migration
5. Switch application to use MySQL adapter

## Troubleshooting

### "Failed to connect to source database"
- Verify host/server name is correct
- Check network connectivity (firewall rules)
- Ensure credentials are correct
- Verify database exists
- Check if database service is running

### "Failed to connect to destination database"
- Same checks as source connection
- Verify user has CREATE TABLE permissions
- Ensure sufficient disk space

### "Destination database already contains data"
- Create a new empty database
- Or manually drop all tables from destination
- Migration requires empty destination for safety

### "Table migration failed"
- Check server activity log for specific error
- Verify data types are compatible
- Check for constraint violations
- Review source data for invalid values

### "Schema creation failed"
- Verify user has DDL permissions
- Check for reserved keyword conflicts
- Review schema converter output
- Check database-specific syntax requirements
