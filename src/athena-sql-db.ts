import {
  AthenaQueryClient,
  AthenaQueryClientConfig,
} from 'athena-query-client';
import {
  generateTableInfoFromTables,
  getTableAndColumnsName,
  SqlTable,
  verifyIgnoreTablesExistInDatabase,
  verifyIncludeTablesExistInDatabase,
  verifyListTablesExistInDatabase,
} from './athena-sql-utils';

export interface AthenaSqlDatabaseDataSourceParams {
  athenaDataSourceConfig: AthenaQueryClientConfig;
  includesTables?: Array<string>;
  ignoreTables?: Array<string>;
  sampleRowsInTableInfo?: number;
  customDescription?: Record<string, string>;
}

/**
 * Class that represents a SQL database from AWS Athena in the LangChain framework.
 *
 * @security **Security Notice**
 * This class generates SQL queries for the given database.
 * The SQLDatabase class provides a getTableInfo method that can be used
 * to get column information as well as sample data from the table.
 * To mitigate risk of leaking sensitive data, limit permissions
 * to read and scope to the tables that are needed.
 * Optionally, use the includesTables or ignoreTables class parameters
 * to limit which tables can/cannot be accessed.
 *
 * @link See https://js.langchain.com/docs/security for more information.
 */
export class AthenaSqlDatabase {
  private athenaDataSource: AthenaQueryClient;
  clientConfig: string;
  database: string;
  catalog: string;
  allTables: Array<SqlTable> = [];
  includesTables: Array<string> = [];
  ignoreTables: Array<string> = [];
  sampleRowsInTableInfo = 3;
  customDescription?: Record<string, string>;

  protected constructor(config: AthenaSqlDatabaseDataSourceParams) {
    this.athenaDataSource = new AthenaQueryClient(
      config.athenaDataSourceConfig,
    );
    if (config?.includesTables && config?.ignoreTables) {
      throw new Error('Cannot specify both include_tables and ignoreTables');
    }
    this.includesTables = config?.includesTables ?? [];
    this.ignoreTables = config?.ignoreTables ?? [];
    this.sampleRowsInTableInfo =
      config?.sampleRowsInTableInfo ?? this.sampleRowsInTableInfo;
  }

  static async fromDataSourceParams(fields: any): Promise<AthenaSqlDatabase> {
    const sqlDatabase = new AthenaSqlDatabase(fields);

    sqlDatabase.allTables = await getTableAndColumnsName(
      sqlDatabase.athenaDataSource,
    );

    verifyIncludeTablesExistInDatabase(
      sqlDatabase.allTables,
      sqlDatabase.includesTables,
    );
    verifyIgnoreTablesExistInDatabase(
      sqlDatabase.allTables,
      sqlDatabase.ignoreTables,
    );
    return sqlDatabase;
  }

  /**
   * Get information about specified tables.
   *
   * Follows best practices as specified in: Rajkumar et al, 2022
   * (https://arxiv.org/abs/2204.00498)
   *
   * If `sample_rows_in_table_info`, the specified number of sample rows will be
   * appended to each table description. This can increase performance as
   * demonstrated in the paper.
   */
  async getTableInfo(targetTables?: Array<string>): Promise<string> {
    let selectedTables =
      this.includesTables.length > 0
        ? this.allTables.filter((currentTable) =>
            this.includesTables.includes(currentTable.tableName),
          )
        : this.allTables;

    if (this.ignoreTables.length > 0) {
      selectedTables = selectedTables.filter(
        (currentTable) => !this.ignoreTables.includes(currentTable.tableName),
      );
    }

    if (targetTables && targetTables.length > 0) {
      verifyListTablesExistInDatabase(
        this.allTables,
        targetTables,
        'Wrong target table name:',
      );
      selectedTables = this.allTables.filter((currentTable) =>
        targetTables.includes(currentTable.tableName),
      );
    }

    return generateTableInfoFromTables(
      selectedTables,
      this.athenaDataSource,
      this.sampleRowsInTableInfo,
      this.customDescription,
    );
  }

  /**
   * Execute a SQL command and return a string representing the results.
   * If the statement returns rows, a string of the results is returned.
   * If the statement returns no rows, an empty string is returned.
   */
  async run(command: string, fetch: 'all' | 'one' = 'all'): Promise<string> {
    const res = await this.athenaDataSource.query(command);

    if (fetch === 'all') {
      return JSON.stringify(res);
    }

    if (res?.length > 0) {
      return JSON.stringify(res[0]);
    }

    return '';
  }
}
