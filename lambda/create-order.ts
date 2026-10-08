import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';
import { randomUUID } from 'crypto';
import { execute } from './db';

const TOPIC_ARN = process.env.TOPIC_ARN!;
const sns = new SNSClient({});
const headers = { 'Content-Type': 'application/json' };

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  try {
    const body = event.body ? JSON.parse(event.body) : {};
    if (!body.item || typeof body.item !== 'string') {
      return respond(400, { message: '"item" (string) is required in the request body' });
    }

    const order = {
      id: randomUUID(),
      item: body.item,
      quantity: body.quantity ?? 1,
      status: 'PENDING',
      createdAt: new Date().toISOString(),
    };

    // 1. Persist the order immediately so GET /orders/{id} works right away.
    await execute({
      sql: `INSERT INTO orders (id, item, quantity, status, created_at)
            VALUES (:id, :item, :quantity, :status, :createdAt::timestamptz)`,
      parameters: [
        { name: 'id', value: { stringValue: order.id } },
        { name: 'item', value: { stringValue: order.item } },
        { name: 'quantity', value: { longValue: order.quantity } },
        { name: 'status', value: { stringValue: order.status } },
        { name: 'createdAt', value: { stringValue: order.createdAt } },
      ],
    });

    // 2. Publish an event announcing the new order. SNS fans this out to
    //    every subscribed queue (processing + audit) without this function
    //    needing to know who's listening.
    await sns.send(
      new PublishCommand({
        TopicArn: TOPIC_ARN,
        Message: JSON.stringify(order),
        MessageAttributes: {
          eventType: { DataType: 'String', StringValue: 'OrderCreated' },
        },
      })
    );

    return respond(202, order); // 202: accepted, processing is async
  } catch (err) {
    console.error(err);
    return respond(500, { message: 'Internal server error' });
  }
};

function respond(statusCode: number, body: unknown): APIGatewayProxyResult {
  return { statusCode, headers, body: JSON.stringify(body) };
}
