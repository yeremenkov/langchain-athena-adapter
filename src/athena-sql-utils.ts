import { AthenaQueryClient } from 'athena-query-client';

interface RawResultTableAndColumn {
  table_name: string;
  column_name: string;
  data_type: string | undefined;
  is_nullable: string;
}

export interface SqlTable {
  tableName: string;
  columns: SqlColumn[];
}

interface SqlColumn {
  columnName: string;
  dataType?: string;
  isNullable?: boolean;
}

export const verifyListTablesExistInDatabase = (
  tablesFromDatabase: Array<SqlTable>,
  listTables: Array<string>,
  errorPrefixMsg: string,
): void => {
  const onlyTableNames: Array<string> = tablesFromDatabase.map(
    (table: SqlTable) => table.tableName,
  );
  if (listTables.length > 0) {
    for (const tableName of listTables) {
      if (!onlyTableNames.includes(tableName)) {
        throw new Error(
          `${errorPrefixMsg} the table ${tableName} was not found in the database`,
        );
      }
    }
  }
};

export const verifyIncludeTablesExistInDatabase = (
  tablesFromDatabase: Array<SqlTable>,
  includeTables: Array<string>,
): void => {
  verifyListTablesExistInDatabase(
    tablesFromDatabase,
    includeTables,
    'Include tables not found in database:',
  );
};

export const verifyIgnoreTablesExistInDatabase = (
  tablesFromDatabase: Array<SqlTable>,
  ignoreTables: Array<string>,
): void => {
  verifyListTablesExistInDatabase(
    tablesFromDatabase,
    ignoreTables,
    'Ignore tables not found in database:',
  );
};

const formatToSqlTable = (
  rawResultsTableAndColumn: Array<RawResultTableAndColumn>,
): Array<SqlTable> => {
  const sqlTable: Array<SqlTable> = [];
  for (const oneResult of rawResultsTableAndColumn) {
    const sqlColumn = {
      columnName: oneResult.column_name,
      dataType: oneResult.data_type,
      isNullable: oneResult.is_nullable === 'YES',
    };
    const currentTable = sqlTable.find(
      (oneTable) => oneTable.tableName === oneResult.table_name,
    );
    if (currentTable) {
      currentTable.columns.push(sqlColumn);
    } else {
      const newTable = {
        tableName: oneResult.table_name,
        columns: [sqlColumn],
      };
      sqlTable.push(newTable);
    }
  }

  return sqlTable;
};

export const getTableAndColumnsName = async (
  appDataSource: AthenaQueryClient,
): Promise<Array<SqlTable>> => {
  const sql =
    'SELECT ' +
    'TABLE_NAME AS table_name, ' +
    'COLUMN_NAME AS column_name, ' +
    'DATA_TYPE AS data_type, ' +
    'IS_NULLABLE AS is_nullable ' +
    'FROM INFORMATION_SCHEMA.COLUMNS ' +
    `WHERE TABLE_SCHEMA = '${appDataSource.database}';`;

  const rep = await appDataSource.query(sql);
  return formatToSqlTable(rep);
};

const formatSqlResponseToSimpleTableString = (rawResult: unknown): string => {
  if (!rawResult || !Array.isArray(rawResult) || rawResult.length === 0) {
    return '';
  }

  let globalString = '';
  for (const oneRow of rawResult) {
    globalString += `${Object.values(oneRow).reduce(
      (completeString, columnValue) => `${completeString} ${columnValue}`,
      '',
    )}\n`;
  }

  return globalString;
};

export const generateTableInfoFromTables = async (
  tables: Array<SqlTable> | undefined,
  appDataSource: any,
  nbSampleRow: number,
  customDescription?: Record<string, string>,
): Promise<string> => {
  if (!tables) {
    return '';
  }

  let globalString = '';
  for (const currentTable of tables) {
    // Add the custom info of the table
    const tableCustomDescription =
      customDescription &&
      Object.keys(customDescription).includes(currentTable.tableName)
        ? `${customDescription[currentTable.tableName]}\n`
        : '';

    // Add the creation of the table in SQL
    const schema = null;
    let sqlCreateTableQuery = schema
      ? `CREATE TABLE "${schema}"."${currentTable.tableName}" (\n`
      : `CREATE TABLE ${currentTable.tableName} (\n`;
    for (const [key, currentColumn] of currentTable.columns.entries()) {
      if (key > 0) {
        sqlCreateTableQuery += ', ';
      }
      sqlCreateTableQuery += `${currentColumn.columnName} ${
        currentColumn.dataType
      } ${currentColumn.isNullable ? '' : 'NOT NULL'}`;
    }
    sqlCreateTableQuery += ') \n';

    const sqlSelectInfoQuery = `SELECT * FROM ${currentTable.tableName} LIMIT ${nbSampleRow};\n`;

    const columnNamesConcatString = `${currentTable.columns.reduce(
      (completeString, column) => `${completeString} ${column.columnName}`,
      '',
    )}\n`;

    let sample = '';
    try {
      const infoObjectResult = nbSampleRow
        ? await appDataSource.query(sqlSelectInfoQuery)
        : null;
      sample = formatSqlResponseToSimpleTableString(infoObjectResult);
    } catch (error) {
      // If the request fails we catch it and only display a log message
      console.log(error);
    }

    globalString = globalString.concat(
      tableCustomDescription +
        sqlCreateTableQuery +
        sqlSelectInfoQuery +
        columnNamesConcatString +
        sample,
    );
  }

  return globalString;
};
