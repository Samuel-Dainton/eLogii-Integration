/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/record', 'N/search', 'N/runtime', 'N/log'],
  function (record, search, runtime, log) {

    const STATUS = {
      PENDING: 1,
      RETRY: 2,
      SUCCESS: 3,
      ERROR: 4,
      PROCESSING: 5,
      PROCESSED: 6
    };

    // ---------- INPUT: find pending NSQ records ----------
    function getInputData() {
      const q = search.create({
        type: 'customrecord_netsuite_queue',
        filters: [
          ['custrecord_nsq_status', 'anyof', [STATUS.PENDING, STATUS.RETRY]]
        ],
        columns: ['internalid']
      });

      const ids = [];
      q.run().each(r => {
        ids.push(r.getValue('internalid'));
        return true;
      });

      log.audit('getInputData', `Found ${ids.length} NSQ records to process`);
      return ids;
    }

    // ---------- MAP: pass id down to reduce ----------
    function map(context) {
      const id = context.value;
      context.write({ key: id, value: id });
    }

    // ---------- REDUCE: process each NSQ record ----------
    function reduce(context) {
      const nsqId = parseInt(context.key, 10);
      try {
        // Load the nsq record
        let nsqRec;
        try {
          nsqRec = record.load({ type: 'customrecord_netsuite_queue', id: nsqId, isDynamic: false });
        } catch (e) {
          log.error('Reduce - load NSQ failed', `NSQ ${nsqId}: ${e.message}`);
          return;
        }

        // read payload
        const rawPayload = nsqRec.getValue('custrecord_nsq_payload');
        if (!rawPayload) {
          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_status: STATUS.ERROR,
              custrecord_nsq_last_error: 'Empty payload'
            }
          });
          log.error('Reduce - empty payload', `NSQ ${nsqId}`);
          return;
        }

        let body;
        try {
          body = JSON.parse(rawPayload);
        } catch (e) {
          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_status: STATUS.ERROR,
              custrecord_nsq_last_error: 'Invalid JSON payload'
            }
          });
          log.error('Reduce - invalid JSON', `NSQ ${nsqId}: ${e.message}`);
          return;
        }

        // --- MUST have externalId per your request ---
        if (!body.externalId) {
          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_status: STATUS.ERROR,
              custrecord_nsq_last_error: 'Missing externalId in payload'
            }
          });
          log.audit('Reduce - missing externalId', `NSQ ${nsqId}`);
          return;
        }

        // Populate NSQ metadata fields
        try {
          const recType = (body.reference && /^RMA/i.test(body.reference)) ? 'RETURN_AUTHORIZATION' :
            (body.reference && /^SO/i.test(body.reference)) ? 'SALES_ORDER' : '';

          // increment attempts
          const prevAttempts = parseInt(nsqRec.getValue('custrecord_nsq_attempts') || 0, 10);
          const attempts = prevAttempts + 1;

          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_record_num: body.reference || '',
              custrecord_nsq_record_id: body.externalId || '',
              custrecord_nsq_context: body.action || '',
              custrecord_nsq_elogii_id: body.uid || '',
              custrecord_nsq_record_type: recType,
              custrecord_nsq_attempts: attempts
            }
          });
        } catch (e) {
          log.error('Reduce - updating NSQ meta failed', e);
          // continue — we'll still try to process the payload below
        }

        // ---------------- Helpers ----------------

        // function findLatestHistoryValue(body, path) {
        //   if (!body || !Array.isArray(body.history)) return null;
        //   const keys = path.split('.');
        //   const latest = [...body.history]
        //     .reverse()
        //     .find(h => {
        //       let current = h;
        //       for (const k of keys) {
        //         if (!current || typeof current !== 'object') return false;
        //         current = current[k];
        //       }
        //       return current !== undefined;
        //     });
        //   if (!latest) return null;
        //   let value = latest;
        //   for (const k of keys) value = value[k];
        //   return value ?? null;
        // }

        // helper: get latest history entry
        function getLatestHistory(b) {
          if (!b || !Array.isArray(b.history) || b.history.length === 0) return null;
          return b.history[b.history.length - 1];
        }

        // // helper: resolve sales order id from payload (externalId or reference -> SOyyy)
        // function resolveSalesOrderId(b) {
        //   if (b.externalId && /^\d+$/.test(String(b.externalId))) {
        //     return parseInt(b.externalId, 10);
        //   } else if (b.reference && /^(ZO|SO)\d+/i.test(b.reference)) {
        //     const numPart = b.reference.replace(/^(ZO|SO)/i, '').trim();
        //     const targetTranId = 'SO' + numPart;
        //     const soSearch = search.create({
        //       type: search.Type.SALES_ORDER,
        //       filters: [['tranid', 'is', targetTranId]],
        //       columns: ['internalid']
        //     }).run().getRange({ start: 0, end: 1 });
        //     if (soSearch && soSearch.length > 0) {
        //       const soId = soSearch[0].getValue('internalid');
        //       return parseInt(soId, 10);
        //     }
        //     return null;
        //   }
        //   return null;
        // }

        // // helper: save with simple retry for RCRD_HAS_BEEN_CHANGED
        // function safeSave(recObj, soId, action, maxRetries = 5, baseWaitMs = 400) {
        //   for (let i = 0; i < maxRetries; i++) {
        //     try {
        //       recObj.save({ enableSourcing: false, ignoreMandatoryFields: true });
        //       return true;
        //     } catch (err) {
        //       if (err && err.name === 'RCRD_HAS_BEEN_CHANGED') {
        //         log.debug('safeSave - retry', `Retry ${i + 1}/${maxRetries} for SO ${soId}`);
        //         if (i < maxRetries - 1) {
        //           const waitMs = baseWaitMs * Math.pow(2, i);
        //           const end = Date.now() + waitMs;
        //           while (Date.now() < end) { } // blocking wait (acceptable here)
        //           try {
        //             recObj = record.load({ type: record.Type.SALES_ORDER, id: soId, isDynamic: false });
        //           } catch (reloadErr) {
        //             log.error('safeSave - reload failed', reloadErr.message);
        //           }
        //         }
        //         continue;
        //       } else {
        //         log.error('safeSave - non-concurrency error', err);
        //         throw err;
        //       }
        //     }
        //   }
        //   log.error('safeSave - failed after retries', soId);
        //   return false;
        // }

        // helper: resolve record type from reference
        function resolveRecordType(ref) {
          if (!ref) return null;
          if (ref.startsWith('SO')) return record.Type.SALES_ORDER;
          if (ref.startsWith('RMA')) return record.Type.RETURN_AUTHORIZATION;
          return null;
        }

        // Safe submitFields with retry logic for concurrency errors.
        function safeSubmitFields(recType, recId, values, action, maxRetries = 5, baseWaitMs = 300) {
          for (let i = 0; i < maxRetries; i++) {
            try {
              record.submitFields({
                type: recType,
                id: recId,
                values: values,
                options: { enableSourcing: false, ignoreMandatoryFields: true }
              });
              return true;
            } catch (err) {

              // Concurrency-safe retry
              if (err.name === 'RCRD_HAS_BEEN_CHANGED') {
                log.debug(
                  'safeSubmitFields retry',
                  `Retry ${i + 1}/${maxRetries} for rec ${recId} action ${action}`
                );

                if (i < maxRetries - 1) {
                  const waitMs = baseWaitMs * Math.pow(2, i);
                  const end = Date.now() + waitMs;
                  while (Date.now() < end) { }
                }

              } else {
                // Non-concurrency error — bail out
                log.error('safeSubmitFields error', err.message);
                throw err;
              }
            }
          }

          log.error('safeSubmitFields failed after all retries', `Record ${recId}`);
          return false;
        }

        // ---------------- End Helpers ----------------

        // --- Main business logic: update record based on body.action ---
        let updated = false;

        // internalid comes straight from eLogii payload
        const recId = body.externalId ? parseInt(body.externalId, 10) : null;
        const recType = resolveRecordType(body.reference || '');

        if (!recId || !recType) {
          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_status: STATUS.PROCESSED,
              custrecord_nsq_last_error: 'Could not resolve target record from payload'
            }
          });
          log.error('Reduce - Record not resolved',
            `NSQ ${nsqId} externalId ${body.externalId} reference ${body.reference}`
          );
          return;
        }

        try {
          //
          // Tasks.assignManually
          //
          if (body.action === 'Tasks.assignManually') {
            const latest = getLatestHistory(body);
            const assigneeInfo = latest?.data?.assignment?.assignee?.info || null;

            if (assigneeInfo) {
              safeSubmitFields(recType, recId, {
                custbody_lap_elogii_trck_link:
                  'https://lapwing.dash-beta.elogii.com/#/tracking?externalId=' +
                  encodeURIComponent(body.externalId || recId),
                custbody_released: true,
                custbody_driver: assigneeInfo.firstName || ''
              });

              updated = true;
              log.debug('Reduce - assignManually applied', `${recType} ${recId}`);
            } else {
              log.audit('Reduce - assignManually missing assigneeInfo', `NSQ ${nsqId}`);
            }
          }

          //
          // Route assignment changes
          //
          else if (
            body.action === 'v3.Optimization.optimizeDates' ||
            body.action === 'Routes.setOrder' ||
            body.action === 'v3.Optimization.optimizeRoutes'
          ) {
            const latest = getLatestHistory(body);
            const routeNumber = latest?.data?.assignment?.routeOrder || null;

            if (routeNumber) {
              safeSubmitFields(recType, recId, {
                custbody_route_stop_num: routeNumber
              });

              updated = true;
              log.debug('Reduce - route info applied',
                `${recType} ${recId} route ${routeNumber}`
              );
            } else {
              log.audit('Reduce - routeNumber missing', `NSQ ${nsqId}`);
            }
          }

          //
          // Move date / update
          //
          else if (body.action === 'Tasks.moveToDate' || body.action === 'Tasks.update') {
            const latest = getLatestHistory(body);
            const lastDeliveryDate = latest?.data?.date || null;

            if (lastDeliveryDate) {
              const ds = String(lastDeliveryDate);
              const y = ds.slice(0, 4);
              const m = ds.slice(4, 6);
              const d = ds.slice(6, 8);

              const formattedDate = new Date(`${y}-${m}-${d}`);

              safeSubmitFields(recType, recId, {
                shipdate: formattedDate
              });

              updated = true;
              log.debug('Reduce - shipdate updated', `${recType} ${recId}`);
            } else {
              log.audit('Reduce - moveToDate missing date', `NSQ ${nsqId}`);
            }
          }

          //
          // Anything else is ignored
          //
          else {
            log.audit('Reduce - Ignored action',
              `NSQ ${nsqId} action ${body.action}`
            );
          }

          //
          // Mark queue processed
          //
          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_status: STATUS.PROCESSED,
              custrecord_nsq_last_error: updated ? '' : 'Processed but no record fields updated'
            }
          });

          log.audit('Reduce - NSQ processed',
            `NSQ ${nsqId}, Record ${recId}, updated=${updated}`
          );
        }

        // --- ERROR HANDLING ---
        catch (err) {
          log.error('Reduce - Record update error', { nsqId, recId, msg: err.message || err });

          const prevAttempts = parseInt(nsqRec.getValue('custrecord_nsq_attempts') || 0, 10);
          const attempts = prevAttempts + 1;

          const newStatus = attempts >= 5 ? STATUS.ERROR : STATUS.RETRY;

          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_attempts: attempts,
              custrecord_nsq_status: newStatus,
              custrecord_nsq_last_error: String(err.message || err)
            }
          });
        }


      } catch (err) {
        log.error('Reduce - general failure', err);
        try {
          record.submitFields({
            type: 'customrecord_netsuite_queue',
            id: nsqId,
            values: {
              custrecord_nsq_status: STATUS.ERROR,
              custrecord_nsq_last_error: String(err.message || err)
            }
          });
        } catch (ee) { log.error('Reduce - failed to mark NSQ error', ee); }
      }
    }

    function summarize(summary) {
      if (summary.inputSummary.error) log.error('Summarize - input error', summary.inputSummary.error);
      summary.mapSummary.errors.iterator().each((key, err) => { log.error('Summarize - map error ' + key, err); return true; });
      summary.reduceSummary.errors.iterator().each((key, err) => { log.error('Summarize - reduce error ' + key, err); return true; });
      log.audit('Summarize', `Usage: ${summary.usage} | Seconds: ${summary.seconds}`);
    }

    return { getInputData, map, reduce, summarize };
  });
