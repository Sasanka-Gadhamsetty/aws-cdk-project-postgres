import { EventBridgeEvent } from 'aws-lambda';
import { execute } from './db';

interface OrderDetail {
  id: string;
  item: string;
}

// Triggered by the EventBridge rule matching source "custom.orders",
// detail-type "OrderProcessed". This is where you'd hang any number of
// independent downstream reactions (notifications, analytics, billing...)
// without process-order.ts needing to know they exist.
export const handler = async (
  event: EventBridgeEvent<'OrderProcessed', OrderDetail>
): Promise<void> => {
  console.log('EventBridge event received:', JSON.stringify(event.detail));

  await execute({
    sql: `UPDATE orders SET audited_at = :auditedAt::timestamptz WHERE id = :id`,
    parameters: [
      { name: 'auditedAt', value: { stringValue: new Date().toISOString() } },
      { name: 'id', value: { stringValue: event.detail.id } },
    ],
  });
};
