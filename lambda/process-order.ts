import { SQSEvent, SQSBatchResponse, SQSBatchItemFailure } from 'aws-lambda';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import { execute } from './db';

const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME!;
const eventBridge = new EventBridgeClient({});

// This function is the "main" consumer of the OrderCreated fan-out: it does
// the actual work (marks the order processed) and then announces completion
// on the custom EventBridge bus, decoupling "processing" from "what happens
// after processing" (e.g. audit logging, notifications, analytics...).
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const batchItemFailures: SQSBatchItemFailure[] = [];

  for (const record of event.Records) {
    try {
      const snsEnvelope = JSON.parse(record.body); // SQS <- SNS envelope
      const order = JSON.parse(snsEnvelope.Message);

      await execute({
        sql: `UPDATE orders
              SET status = :status, processed_at = :processedAt::timestamptz
              WHERE id = :id`,
        parameters: [
          { name: 'status', value: { stringValue: 'PROCESSED' } },
          { name: 'processedAt', value: { stringValue: new Date().toISOString() } },
          { name: 'id', value: { stringValue: order.id } },
        ],
      });

      await eventBridge.send(
        new PutEventsCommand({
          Entries: [
            {
              EventBusName: EVENT_BUS_NAME,
              Source: 'custom.orders',
              DetailType: 'OrderProcessed',
              Detail: JSON.stringify(order),
            },
          ],
        })
      );

      console.log(`Order ${order.id} processed`);
    } catch (err) {
      console.error('Failed to process record', record.messageId, err);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  // Returning failed message IDs lets SQS retry only those, not the whole batch.
  return { batchItemFailures };
};
