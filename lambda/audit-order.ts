import { SQSEvent, SQSBatchResponse, SQSBatchItemFailure } from 'aws-lambda';

// This is the second subscriber on the same SNS topic as process-order.ts.
// It runs completely independently — its own queue, own retry behavior, own
// failures — demonstrating SNS fan-out to multiple SQS consumers.
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  const batchItemFailures: SQSBatchItemFailure[] = [];

  for (const record of event.Records) {
    try {
      const snsEnvelope = JSON.parse(record.body);
      const order = JSON.parse(snsEnvelope.Message);
      console.log('AUDIT: order created', {
        id: order.id,
        item: order.item,
        createdAt: order.createdAt,
      });
    } catch (err) {
      console.error('Failed to audit record', record.messageId, err);
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  return { batchItemFailures };
};
