/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 */
define(['N/log', 'N/record', 'N/runtime', 'N/search'], (log, record, runtime, search) => {

  function onRequest(context) {
    const SECRET_KEY = runtime.getCurrentScript().getParameter({ name: 'custscript_elogii_secret_key' });
    const method = context.request.method;

    const STATUS = {
      PENDING: 1,
      RETRY: 2,
      SUCCESS: 3,
      ERROR: 4,
      PROCESSING: 5,
      PROCESSED: 6
    };

    if (method !== 'POST') {
      context.response.statusCode = 405;
      context.response.write('Method Not Allowed');
      return;
    }

    const incomingKey = context.request.headers['x-api-key'];
    if (incomingKey !== SECRET_KEY) {
      context.response.statusCode = 403;
      context.response.write('Forbidden: Invalid API Key');
      return;
    }

    let body;
    try {
      body = JSON.parse(context.request.body);
    } catch (e) {
      log.error('Invalid JSON', e.message);
      context.response.statusCode = 400;
      context.response.write('Invalid JSON');
      return;
    }

    // --- Must have externalId ---
    if (!body.externalId) {
      log.audit('Webhook Ignored - Missing externalId', `Body: ${body.externalId}, Reference: ${body.reference}`);
      context.response.write(JSON.stringify({
        success: false,
        message: 'Ignored — externalId missing'
      }));
      return;
    }

    try {
      const allowedActions = [
        'Tasks.assignManually',
        'Tasks.moveToDate',
        'Tasks.update',
        'v3.Optimization.optimizeDates',
        'v3.Optimization.optimizeRoutes',
        'Routes.setOrder',
        'Routes.reassign',
        'Routes.swap'
      ];

      if (!allowedActions.includes(body.action)) {

        if (body.action !== 'Routes.updateETAs') {

          try {
            const nsq = record.create({
              type: 'customrecord_netsuite_queue',
              isDynamic: false
            });

            nsq.setValue({
              fieldId: 'custrecord_nsq_payload',
              value: JSON.stringify(body)
            });

            nsq.setValue({
              fieldId: 'custrecord_nsq_status',
              value: STATUS.ERROR
            });

            nsq.setValue({
              fieldId: 'custrecord_nsq_context',
              value: `Ignored webhook action: ${body.action}`
            });

            const nsqId = nsq.save();
            log.debug('NetSuite Queue Created', `Queue ID ${nsqId} for ExternalId ${body.externalId}`);
          } catch (e) {
            log.error('Failed to create NetSuite Queue', {
              message: e.message,
              body: JSON.stringify(body)
            });

            context.response.write(JSON.stringify({
              success: false,
              error: 'Failed to create NetSuite queue record',
              details: e.message
            }));
          }
        }
        return;
      }

      // ---- Determine if payload contains anything worth queueing ----
      // helper: get latest history entry
      function getLatestHistory(b) {
        if (!b || !Array.isArray(b.history) || b.history.length === 0) return null;
        return b.history[b.history.length - 1];
      }

      const latest = getLatestHistory(body);
      let shouldCreate = false;

      if (body.action === 'Tasks.assignManually') {
        shouldCreate = !!(latest?.data?.assignment?.assignee?.info);
      }

      else if (
        body.action === 'v3.Optimization.optimizeDates' ||
        body.action === 'Routes.setOrder' ||
        body.action === 'v3.Optimization.optimizeRoutes' ||
        body.action === 'Routes.reassign' ||
        body.action === 'Routes.swap'
      ) {
        shouldCreate = !!(latest?.data?.assignment?.routeOrder);
        shouldCreate = !!(latest?.data?.assignment?.assignee?.info);
      }

      else if (body.action === 'Tasks.moveToDate' || body.action === 'Tasks.update') {
        shouldCreate = !!(latest?.data?.date);
      }

      if (!shouldCreate) {
        log.debug('Webhook skipped – missing required fields', {
          reference: body.reference,
          action: body.action,
          needed:
            body.action === 'Tasks.assignManually' ? 'assigneeInfo' :
              body.action.includes('optimize') || body.action === 'Routes.setOrder' ? 'routeOrder' :
                'date'
        });
        return;
      }

    } catch (e) {
      log.error('Malformed body', e.message);
      context.response.statusCode = 400;
      context.response.write('Malformed body');
      return;
    }

    // --- NEW: Create NetSuite Queue Record ---
    try {
      const nsq = record.create({
        type: 'customrecord_netsuite_queue',
        isDynamic: false
      });

      nsq.setValue({
        fieldId: 'custrecord_nsq_payload',
        value: JSON.stringify(body)
      });

      nsq.setValue({
        fieldId: 'custrecord_nsq_status',
        value: STATUS.PENDING
      });

      const nsqId = nsq.save();
      log.debug('NetSuite Queue Created', `Queue ID ${nsqId} for ExternalId ${body.externalId}`);
    } catch (e) {
      log.error('Failed to create NetSuite Queue', {
        message: e.message,
        body: JSON.stringify(body)
      });

      context.response.write(JSON.stringify({
        success: false,
        error: 'Failed to create NetSuite queue record',
        details: e.message
      }));
    }
  }

  return { onRequest };
});
