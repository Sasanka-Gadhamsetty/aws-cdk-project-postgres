import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { execute, parseRows } from './db';

const headers = { 'Content-Type': 'application/json' };

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  try {
    const id = event.pathParameters?.id;
    if (!id) {
      return respond(400, { message: 'Missing order id in path' });
    }

    const result = await execute({
      sql: `SELECT id, item, quantity, status, created_at, processed_at, audited_at
            FROM orders WHERE id = :id`,
      parameters: [{ name: 'id', value: { stringValue: id } }],
    });

    const rows = parseRows<Record<string, unknown>>(result);
    if (rows.length === 0) {
      return respond(404, { message: `Order ${id} not found` });
    }

    return respond(200, rows[0]);
  } catch (err) {
    console.error(err);
    return respond(500, { message: 'Internal server error' });
  }
};

function respond(statusCode: number, body: unknown): APIGatewayProxyResult {
  return { statusCode, headers, body: JSON.stringify(body) };
}
