import { RDSDataClient, ExecuteStatementCommand, SqlParameter } from '@aws-sdk/client-rds-data';

const client = new RDSDataClient({});

const CLUSTER_ARN = process.env.CLUSTER_ARN!;
const SECRET_ARN = process.env.SECRET_ARN!;
const DATABASE_NAME = process.env.DATABASE_NAME!;

export interface QueryInput {
  sql: string;
  parameters?: SqlParameter[];
}

/**
 * Runs a statement against the Aurora Postgres cluster via the RDS Data API
 * (i.e. over HTTPS, no DB connection/VPC networking needed from Lambda).
 * formatRecordsAs: 'JSON' makes the result come back as a ready-to-parse
 * JSON array instead of the raw typed-field format Data API returns by default.
 */
export async function execute({ sql, parameters }: QueryInput) {
  return client.send(
    new ExecuteStatementCommand({
      resourceArn: CLUSTER_ARN,
      secretArn: SECRET_ARN,
      database: DATABASE_NAME,
      sql,
      parameters,
      formatRecordsAs: 'JSON',
    })
  );
}

export function parseRows<T>(result: { formattedRecords?: string }): T[] {
  return result.formattedRecords ? JSON.parse(result.formattedRecords) : [];
}
