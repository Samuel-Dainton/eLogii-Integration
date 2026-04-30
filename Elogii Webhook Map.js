/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/record', 'N/search', 'N/log'],
  (record, search, log) => {

    const STATUS = {
      PENDING: 1,
      RETRY: 2,
      SUCCESS: 3,
      ERROR: 4,
      PROCESSING: 5,
      PROCESSED: 6
    };

    // ---------------- INPUT ----------------
    function getInputData() {
      return search.create({
        type: 'customrecord_netsuite_queue',
        filters: [['custrecord_nsq_status', 'anyof', [STATUS.PENDING, STATUS.RETRY]]],
        columns: [
          'internalid',
          'custrecord_nsq_payload',
          'custrecord_nsq_attempts'
        ]
      });
    }

    // ---------------- MAP (MAIN PROCESSING) ----------------
    function map(context) {
      const row = JSON.parse(context.value);

      const nsqId = row.id;
      const payload = row.values.custrecord_nsq_payload;
      const attempts = parseInt(row.values.custrecord_nsq_attempts || 0, 10);

      if (!payload) {
        return markError(nsqId, 'Empty payload');
      }

      let body;
      try {
        body = JSON.parse(payload);
      } catch (e) {
        return markError(nsqId, 'Invalid JSON');
      }

      try {
        if (isOptimizationPayload(body)) {
          processOptimization(nsqId, body);
        } else {
          processStandard(nsqId, body, attempts);
        }
      } catch (err) {
        handleFailure(nsqId, attempts, err);
      }
    }

    // ---------------- SUMMARIZE ----------------
    function summarize(summary) {
      if (summary.inputSummary.error) log.error('Input error', summary.inputSummary.error);
      summary.mapSummary.errors.iterator().each((key, err) => {
        log.error('Map error', { key, err });
        return true;
      });
      log.audit('Summarize', `Seconds: ${summary.seconds}`);
    }

    // ---------------- OPTIMIZATION PAYLOAD ----------------
    function processOptimization(nsqId, body) {
      const updates = {}; // { recId: { type, values } }

      body.routes?.forEach(route => {
        const driverName = route?.assignee?.info?.firstName;

        route.legs?.forEach(leg => {
          if (!leg.taskInfo?.externalId) return;

          const recId = parseInt(leg.taskInfo.externalId, 10);
          const recType = resolveRecordType(leg.taskInfo.reference);

          if (!recId || !recType) return;

          if (!updates[recId]) {
            updates[recId] = { type: recType, values: {} };
          }

          const values = updates[recId].values;

          if (leg.routeOrder !== undefined) {
            values.custbody_route_stop_num = leg.routeOrder;
          }

          if (leg.loadingOrder !== undefined) {
            values.custbody_load_order = leg.loadingOrder;
          }

          if (driverName) {
            values.custbody_driver = driverName;
          }
        });
      });

      // 🔥 Single submit per record
      Object.keys(updates).forEach(recId => {
        safeSubmitFields(
          updates[recId].type,
          recId,
          updates[recId].values
        );
      });

      markProcessed(nsqId);
    }

    // ---------------- STANDARD PAYLOAD ----------------
    function processStandard(nsqId, body, attempts) {

      if (!body.externalId) {
        return markError(nsqId, 'Missing externalId');
      }

      const recId = parseInt(body.externalId, 10);
      const recType = resolveRecordType(body.reference);

      if (!recId || !recType) {
        return markProcessed(nsqId, 'Record not resolved');
      }

      const latest = getLatestHistory(body);
      const values = {};

      if (body.action === 'Tasks.assignManually') {

        const assignee = latest?.data?.assignment?.assignee?.info;

        if (assignee?.firstName) {
          values.custbody_driver = assignee.firstName;
        }

        const routeOrder = findLatestAssignmentValue(body, 'routeOrder');
        const loadOrder = findLatestAssignmentValue(body, 'loadingOrder');

        if (routeOrder != null) values.custbody_route_stop_num = routeOrder;
        if (loadOrder != null) values.custbody_load_order = loadOrder;

        if (hasCourierSkill(assignee)) {
          values.custbody_for_courier = true;
        }

        values.custbody_released = true;
      }

      else if (
        body.action === 'Routes.setOrder' ||
        body.action === 'Routes.reassign' ||
        body.action === 'Routes.swap' ||
        body.action === 'v3.Optimization.optimizeRoutes'
      ) {

        const assignee = latest?.data?.assignment?.assignee?.info;

        const routeOrder = findLatestAssignmentValue(body, 'routeOrder');
        const loadOrder = findLatestAssignmentValue(body, 'loadingOrder');

        if (routeOrder != null) values.custbody_route_stop_num = routeOrder;
        if (loadOrder != null) values.custbody_load_order = loadOrder;
        if (assignee?.firstName) values.custbody_driver = assignee.firstName;

        if (hasCourierSkill(assignee)) {
          values.custbody_for_courier = true;
        }
      }

      else if (body.action === 'Tasks.moveToDate' || body.action === 'Tasks.update') {
        const date = latest?.data?.date;

        if (date) {
          const ds = String(date);
          const y = ds.slice(0, 4);
          const m = ds.slice(4, 6);
          const d = ds.slice(6, 8);
          values.shipdate = new Date(`${y}-${m}-${d}`);
        }
      }

      if (Object.keys(values).length) {
        safeSubmitFields(recType, recId, values);
      }

      markProcessed(nsqId);
    }

    // ---------------- HELPERS ----------------

    function safeSubmitFields(type, id, values) {
      try {
        record.submitFields({
          type,
          id,
          values,
          options: { enableSourcing: false, ignoreMandatoryFields: true }
        });
      } catch (e) {
        if (e.name === 'RCRD_HAS_BEEN_CHANGED') {
          throw e; // let retry system handle it
        }
        throw e;
      }
    }

    function markProcessed(id, msg = '') {
      record.submitFields({
        type: 'customrecord_netsuite_queue',
        id,
        values: {
          custrecord_nsq_status: STATUS.PROCESSED,
          custrecord_nsq_last_error: msg
        }
      });
    }

    function markError(id, msg) {
      record.submitFields({
        type: 'customrecord_netsuite_queue',
        id,
        values: {
          custrecord_nsq_status: STATUS.ERROR,
          custrecord_nsq_last_error: msg
        }
      });
    }

    function handleFailure(id, attempts, err) {
      const nextStatus = attempts + 1 >= 5 ? STATUS.ERROR : STATUS.RETRY;

      record.submitFields({
        type: 'customrecord_netsuite_queue',
        id,
        values: {
          custrecord_nsq_status: nextStatus,
          custrecord_nsq_attempts: attempts + 1,
          custrecord_nsq_last_error: err.message || err
        }
      });
    }

    function resolveRecordType(ref) {
      if (!ref) return null;
      if (ref.startsWith('SO')) return record.Type.SALES_ORDER;
      if (ref.startsWith('RMA')) return record.Type.RETURN_AUTHORIZATION;
      return null;
    }

    function getLatestHistory(b) {
      return b?.history?.length ? b.history[b.history.length - 1] : null;
    }

    function findLatestAssignmentValue(body, field) {
      if (!body?.history) return null;

      for (let i = body.history.length - 1; i >= 0; i--) {
        const val = body.history[i]?.data?.assignment?.[field];
        if (val != null) return val;
      }
      return null;
    }

    function isOptimizationPayload(body) {
      return Array.isArray(body.routes);
    }

    function hasCourierSkill(info) {
      return info?.skills?.some(s => String(s).toLowerCase() === 'courier');
    }

    return { getInputData, map, summarize };
  });
