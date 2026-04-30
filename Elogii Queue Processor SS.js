/**
 * @NApiVersion 2.1
 * @NScriptType ScheduledScript
 */
define(['N/search', 'N/record', 'N/https', 'N/runtime', 'N/task', 'N/log'],
    function (search, record, https, runtime, task, log) {

        const MAX_PER_RUN = 200;
        const MAX_ATTEMPTS = 3;

        const STATUS = {
            PENDING: 1,
            RETRY: 2,
            SUCCESS: 3,
            ERROR: 4,
            PROCESSING: 5,
            PROCESSED: 6
        };

        function execute(context) {
            try {
                // 1) Query queue records due for processing (PROCESSED or RETRY and next_run <= today)
                const qSearch = search.create({
                    type: 'customrecord_elogii_queue',
                    filters: [
                        ['custrecord_elq_next_run', 'onorbefore', 'today'],
                        'AND',
                        ['custrecord_elq_status', 'anyof', STATUS.PROCESSED, STATUS.RETRY]
                    ],
                    columns: [
                        'internalid',
                        'custrecord_elq_so_id',
                        'custrecord_elq_attempts',
                        'custrecord_elq_payload',
                        'custrecord_elq_context',
                        'custrecord_elq_elogii_id',
                        'custrecord_elq_record_type',
                        'custrecord_elq_elogii_internal_id'
                    ]
                });

                const results = qSearch.run().getRange({ start: 0, end: MAX_PER_RUN }) || [];
                log.audit('QueueProcessor (batch)', `Found ${results.length} items`);

                if (!results || results.length === 0) return;

                const creds = getElogiiApiKeyAndTaskURL();
                const apiKey = creds.apiKey;
                const baseUrl = creds.elogiiURL; // e.g. https://api-sandbox.elogii.com/tasks
                var anyRetry = false;

                if (!apiKey || !baseUrl) {
                    log.error('Missing credentials', 'Elogii API key or base URL missing');
                    return;
                }

                // Partition items by context
                const groups = { create: [], edit: [], delete: [] };
                const mappings = { create: [], edit: [], delete: [] };

                // Build groups (and stable mapping / matchKey)
                for (let i = 0; i < results.length; i++) {
                    const r = results[i];
                    const qId = r.getValue('internalid');
                    const rawPayload = r.getValue('custrecord_elq_payload') || '';
                    const ctxRaw = r.getValue('custrecord_elq_context') || '';
                    const elogiiId = r.getValue('custrecord_elq_elogii_id') || null;
                    const soId = r.getValue('custrecord_elq_so_id') || null;
                    const elogiiInternalId = r.getValue('custrecord_elq_elogii_internal_id') || null;
                    const recordType = r.getValue('custrecord_elq_record_type') || record.Type.SALES_ORDER;
                    const attempts = parseInt(r.getValue('custrecord_elq_attempts') || 0, 10);

                    // tolerate empty payloads
                    if (!rawPayload) {
                        markError(qId, 'Empty payload');
                        continue;
                    }

                    let parsed;
                    try {
                        parsed = JSON.parse(rawPayload);
                    } catch (e) {
                        // If payload is plain string (e.g. previously set to an id), accept it as wrapper
                        parsed = rawPayload;
                    }

                    // payload may be stored as { payload: {...}, elogiiId: 'T-...' } or just payload object
                    const payload = (typeof parsed === 'object' && parsed !== null && parsed.payload) ? parsed.payload : parsed;
                    const wrappedElogiiId = (typeof parsed === 'object' && parsed !== null && (parsed.elogiiId || parsed.elogiiid)) ? (parsed.elogiiId || parsed.elogiiid) : (elogiiId || null);

                    // Normalize context
                    const ctx = String(ctxRaw || '').toLowerCase();
                    const ctxNorm =
                        (ctx === 'edit' || ctx === 'update')
                            ? 'edit'
                            : (ctx === 'delete' ? 'delete' : 'create');

                    // Build matchKey and push into groups/mappings
                    if (ctxNorm === 'create') {
                        // matchKey: payload.externalId || payload.reference || soId || qId
                        const mk = (payload && (payload.externalId || payload.reference)) || (soId ? String(soId) : null) || String(qId);
                        groups.create.push(payload);
                        mappings.create.push({ qId, soId, matchKey: mk, origElogiiId: wrappedElogiiId, attempts });
                    } else if (ctxNorm === 'edit') {
                        const uid = wrappedElogiiId || (payload && (payload.uid || payload.externalId)) || null;
                        const mk = uid || (payload && payload.externalId) || (soId ? String(soId) : null) || String(qId);
                        const item = Object.assign({}, payload);
                        if (uid) item.uid = uid;
                        groups.edit.push(item);
                        mappings.edit.push({ qId, soId, uid, matchKey: mk, attempts });
                    } else if (ctxNorm === 'delete') {
                        // For delete we prefer internal ID (_id) if present. If only uid exist, use that.
                        if (elogiiInternalId) {
                            groups.delete.push(String(elogiiInternalId)); // **IMPORTANT**: send array of strings for removeMany
                            mappings.delete.push({ qId, soId, _id: String(elogiiInternalId), matchKey: String(elogiiInternalId), attempts });
                        } else if (wrappedElogiiId) {
                            // we can also remove by uid
                            groups.delete.push(String(wrappedElogiiId));
                            mappings.delete.push({ qId, soId, uid: String(wrappedElogiiId), matchKey: String(wrappedElogiiId), attempts });
                        } else {
                            // No ID present: nothing to delete on eLogii; mark success / cleanup
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: qId,
                                values: {
                                    custrecord_elq_status: STATUS.SUCCESS,
                                    custrecord_elq_last_error: 'No eLogii uid present for delete — cleaned up locally'
                                }
                            });
                        }
                    }
                } // end partition

                // 2) Process each group via the respective many endpoint
                // Process order: creates, edits, deletes (keeps your previous order)
                if (groups.create.length > 0) {
                    processBatch('create', groups.create, mappings.create, baseUrl, apiKey);
                }
                if (groups.edit.length > 0) {
                    processBatch('edit', groups.edit, mappings.edit, baseUrl, apiKey);
                }
                if (groups.delete.length > 0) {
                    processBatch('delete', groups.delete, mappings.delete, baseUrl, apiKey);
                }

                // If we processed MAX_PER_RUN then reschedule (keeps behaviour)
                if (results.length === MAX_PER_RUN) {
                    try {
                        const s = task.create({ taskType: task.TaskType.SCHEDULED_SCRIPT, scriptId: runtime.getCurrentScript().id, deploymentId: runtime.getCurrentScript().deploymentId });
                        s.submit();
                        log.audit('QueueProcessor', 'Rescheduled self (more work likely)');
                    } catch (err) {
                        log.error('QueueProcessor reschedule failed', err);
                    }
                }

                rescheduleIfNeeded(anyRetry);

            } catch (e) {
                log.error('QueueProcessor fatal', e);
            }
        } // execute

        // ----------------------
        // Batch processing helper
        // ----------------------
        function processBatch(type, itemsArray, mappingArray, baseUrl, apiKey) {
            try {
                // Compose endpoint
                const endpointMap = {
                    create: `${baseUrl}/createMany`,
                    edit: `${baseUrl}/updateMany`,
                    delete: `${baseUrl}/removeMany`
                };
                const url = endpointMap[type];
                if (!url) {
                    log.error('processBatch', `Unknown batch type ${type}`);
                    return;
                }

                // Save debug URL on first mapping item (so you can inspect)
                if (mappingArray && mappingArray.length > 0) {
                    recordDebugUrl(mappingArray[0].qId, url);
                }

                // Build request body
                let bodyObj;
                if (type === 'create') {
                    bodyObj = { items: itemsArray, returnItems: true };
                } else if (type === 'edit') {
                    bodyObj = { items: itemsArray, returnItems: true };
                } else if (type === 'delete') {
                    // itemsArray is array of string ids (uids or _ids)
                    bodyObj = { ids: itemsArray };
                }

                const resp = https.post({
                    url: url,
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `ApiKey ${apiKey}`
                    },
                    body: JSON.stringify(bodyObj)
                });

                if (!resp) {
                    // network/no response — mark all Retry
                    log.error('Batch no response', `type=${type} len=${itemsArray.length}`);
                    for (const m of mappingArray) markRetryGeneric(m.qId, m, 'No response from eLogii');
                    return;
                }

                const code = resp.code;
                let respBody = null;
                try { respBody = resp.body ? JSON.parse(resp.body) : null; } catch (e) { respBody = { raw: resp.body }; }

                // Save raw response for debugging (truncated)
                // try {
                //     const rawResp = JSON.stringify(respBody).substring(0, 10000);
                //     for (const mapping of mappingArray) {
                //         record.submitFields({
                //             type: 'customrecord_elogii_queue',
                //             id: mapping.qId,
                //             values: { custrecord_elq_response_json: rawResp }
                //         });
                //     }
                // } catch (e) {
                //     log.error('Failed to save response body to NSQ', e);
                // }

                // handle 429
                if (code === 429) {
                    log.audit('Batch 429', `type=${type} count=${mappingArray.length}`);
                    for (const m of mappingArray) markRetryGeneric(m.qId, m, '429 Too Many Requests');
                    return;
                }

                // 2xx success. Handle per-type structure
                if (code >= 200 && code < 300) {
                    log.audit('Batch success', `type=${type} count=${mappingArray.length}`);
                    // -------- createMany response handling --------
                    if (type === 'create') {
                        // Expect respBody.items array with created task objects
                        if (respBody && Array.isArray(respBody.items) && respBody.items.length > 0) {

                            // Build mapByKey from mappingArray using matchKey (defined earlier)
                            const mapByKey = {};
                            for (let i = 0; i < mappingArray.length; i++) {
                                const m = mappingArray[i];
                                const key = (m.matchKey || m.uid || (m.soId ? String(m.soId) : null) || String(m.qId));
                                if (!mapByKey[key]) mapByKey[key] = [];
                                mapByKey[key].push(m);
                            }

                            // iterate returned items and match them
                            for (const returned of respBody.items) {
                                const returnedUid = returned.uid || returned._id || returned.externalId || returned.reference;
                                const returnedInternalId = returned._id || null;
                                const returnedExternalId = returned.externalId || null;
                                const returnedReference = returned.reference || null;

                                // build candidate keys
                                const candidates = [];
                                if (returnedUid) candidates.push(String(returnedUid));
                                if (returnedExternalId) candidates.push(String(returnedExternalId));
                                if (returnedReference) candidates.push(String(returnedReference));
                                // also support 'SO123' matching
                                if (returnedReference) candidates.push(String(returnedReference).toUpperCase());
                                // fallback to trying to match any mapping with same soId (if only one mapping)
                                let matched = null;
                                for (const k of candidates) {
                                    if (mapByKey[k] && mapByKey[k].length > 0) {
                                        matched = mapByKey[k].shift();
                                        break;
                                    }
                                }

                                if (!matched) {
                                    // fallback: if only one mapping, use it
                                    if (mappingArray.length === 1) {
                                        matched = mappingArray[0];
                                    } else {
                                        log.audit('Unmatched returned item', { returned });
                                        continue;
                                    }
                                }

                                // update queue record with returned ids and mark success
                                try {
                                    record.submitFields({
                                        type: 'customrecord_elogii_queue',
                                        id: matched.qId,
                                        values: {
                                            custrecord_elq_elogii_id: String(returnedUid || returnedExternalId || returnedInternalId),
                                            custrecord_elq_elogii_internal_id: String(returnedInternalId || ''),
                                            custrecord_elq_status: STATUS.SUCCESS,
                                            custrecord_elq_last_error: ''
                                        }
                                    });
                                } catch (e) {
                                    log.error('Failed to update queue record for returned item', { qId: matched.qId, err: e });
                                }

                                // update originating transaction (if available)
                                if (matched.soId) {
                                    log.audit('Updating originating transaction with eLogii IDs', { soId: matched.soId, returnedUid, returnedInternalId });
                                    const soIdToUpdate = parseInt(matched.soId, 10);
                                    const recordType =
                                        (returnedReference || '').toUpperCase().startsWith('RMA') ? record.Type.RETURN_AUTHORIZATION :
                                            (returnedReference || '').toUpperCase().startsWith('SO') ? record.Type.SALES_ORDER :
                                                record.Type.SALES_ORDER;

                                    try {
                                        record.submitFields({
                                            type: recordType,
                                            id: soIdToUpdate,
                                            values: {
                                                custbody_lap_elogii_id: String(returnedUid),
                                                custbody_elogii_internal_id: String(returnedInternalId || ''),
                                                custbody_lap_elogii_trck_link: `https://lapwing.dash-beta.elogii.com/#/tracking?uid=${returnedUid}`,
                                                custbody_lap_elogii_task_status: 'Elogii Task Created'
                                            }
                                        });
                                    } catch (e) {
                                        log.error('Could not update originating transaction with UID', { soId: matched.soId, err: e.message || e });
                                    }
                                } else {
                                    log.audit('Returned item had no matched soId', { returned, matched });
                                }
                            }

                            // Mark any leftover mapping entries as ERROR (not returned)
                            for (const k in mapByKey) {
                                const arr = mapByKey[k];
                                for (const leftover of arr) {
                                    try {
                                        record.submitFields({
                                            type: 'customrecord_elogii_queue',
                                            id: leftover.qId,
                                            values: {
                                                custrecord_elq_status: STATUS.ERROR,
                                                custrecord_elq_last_error: `createMany did not return item for matchKey ${k}`
                                            }
                                        });
                                    } catch (e) {
                                        log.error('Failed marking leftover mapping as ERROR', { qId: leftover.qId, err: e });
                                    }
                                    log.warn('Batch item not returned by eLogii, marking ERROR', { qId: leftover.qId, matchKey: k });
                                }
                            }

                            return;
                        }
                        // If no items in response, fall through to ambiguous handling below
                    } // end create

                    // -------- updateMany/removeMany handling (edit/delete) --------
                    // If respBody contains per-item errors or ids, we process per index
                    if (respBody && (Array.isArray(respBody.errors) || respBody.result || respBody.ids)) {
                        // map per-index
                        const errors = Array.isArray(respBody.errors) ? respBody.errors : [];
                        for (let i = 0; i < mappingArray.length; i++) {
                            const mapping = mappingArray[i];
                            const itemError = errors[i] || null;
                            if (itemError) {
                                // mark error
                                record.submitFields({
                                    type: 'customrecord_elogii_queue',
                                    id: mapping.qId,
                                    values: {
                                        custrecord_elq_status: STATUS.ERROR,
                                        custrecord_elq_last_error: `batch ${type} item error: ${JSON.stringify(itemError)}`
                                    }
                                });
                            } else {
                                // success
                                record.submitFields({
                                    type: 'customrecord_elogii_queue',
                                    id: mapping.qId,
                                    values: {
                                        custrecord_elq_status: STATUS.SUCCESS,
                                        custrecord_elq_last_error: `${type} success`
                                    }
                                });
                            }
                        }
                        return;
                    }

                    // ambiguous success (no per-item info) - mark all success
                    for (const mapping of mappingArray) {
                        try {
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: mapping.qId,
                                values: { custrecord_elq_status: STATUS.SUCCESS, custrecord_elq_last_error: '' }
                            });
                        } catch (e) {
                            log.error('Batch ambiguous result update fail', { qId: mapping.qId, err: e });
                        }
                    }
                    return;
                }

                // Non 2xx non-429 responses (client errors)
                if (code >= 400 && code < 500) {
                    log.error('Batch client error', { type, code, body: resp.body });
                    for (const m of mappingArray) {
                        record.submitFields({
                            type: 'customrecord_elogii_queue',
                            id: m.qId,
                            values: {
                                custrecord_elq_status: STATUS.ERROR,
                                custrecord_elq_last_error: `HTTP ${code}: ${resp.body}`
                            }
                        });
                    }
                    return;
                }

                // 5xx and other server errors — mark retry for all with backoff
                log.error('Batch server error', { type, code, body: resp.body });
                for (const m of mappingArray) markRetryGeneric(m.qId, m, `HTTP ${code}`);

            } catch (e) {
                log.error('processBatch exception', e);
                try {
                    for (const m of mappingArray) markRetryGeneric(m.qId, m, 'processBatch exception: ' + String(e));
                } catch (err) {
                    log.error('processBatch markRetryGeneric failed', err);
                }
            }
        } // processBatch

        // ----------------------
        // helper: mark retry
        // ----------------------
        function markRetryGeneric(qId, mappingItem, msg) {
            try {
                anyRetry = true;
                const attempts = (parseInt(mappingItem.attempts || 0, 10)) + 1;
                const backoffSec = Math.pow(2, attempts) * 60;
                const nextRun = new Date(Date.now() + backoffSec * 1000);
                record.submitFields({
                    type: 'customrecord_elogii_queue',
                    id: qId,
                    values: {
                        custrecord_elq_attempts: attempts,
                        custrecord_elq_next_run: nextRun,
                        custrecord_elq_status: STATUS.RETRY,
                        custrecord_elq_last_error: String(msg)
                    }
                });
            } catch (e) {
                log.error('markRetryGeneric failed', { qId, err: e });
            }
        }

        function markError(qId, msg) {
            try {
                record.submitFields({
                    type: 'customrecord_elogii_queue',
                    id: qId,
                    values: {
                        custrecord_elq_status: STATUS.ERROR,
                        custrecord_elq_last_error: String(msg)
                    }
                });
            } catch (e) {
                log.error('markError failed', { qId, err: e });
            }
        }

        function recordDebugUrl(qId, urlString) {
            try {
                record.submitFields({
                    type: 'customrecord_elogii_queue',
                    id: qId,
                    values: {
                        custrecord_elq_debug_url: `DEBUG URL: ${urlString}`
                    }
                });
            } catch (e) {
                log.error("Error saving debug URL", e);
            }
        }

        function getElogiiApiKeyAndTaskURL() {
            var obj = {};
            try {
                const customrecord_deploymentparametersSearchObj = search.create({
                    type: "customrecord_deploymentparameters",
                    filters: [["name", "haskeywords", "Elogii"]],
                    columns: [
                        search.createColumn({ name: "custrecord_deploymentparametervalue" }),
                        search.createColumn({ name: "custrecord_deploymentparametervalue2" })
                    ]
                });

                customrecord_deploymentparametersSearchObj.run().each(result => {
                    const v1 = result.getValue({ name: 'custrecord_deploymentparametervalue' });
                    const v2 = result.getValue({ name: 'custrecord_deploymentparametervalue2' });
                    if (v1) obj.prodAPIkey = v1;
                    if (v2) obj.sandboxAPIkey = v2;
                    return true;
                });

                const env = runtime.envType;
                obj.apiKey = env === 'SANDBOX' ? obj.sandboxAPIkey : obj.prodAPIkey;
                obj.elogiiURL = env === 'SANDBOX' ? 'https://api-sandbox.elogii.com/tasks' : 'https://api-35.elogii.com/tasks';
                return obj;
            } catch (err) {
                log.error('getElogiiApiKeyAndTaskURL error', err);
                return obj;
            }
        }

        function rescheduleIfNeeded(anyRetryFlag) {
            if (!anyRetryFlag) return;
            try {
                var scriptTask = task.create({
                    taskType: task.TaskType.SCHEDULED_SCRIPT
                });
                scriptTask.scriptId = runtime.getCurrentScript().id;
                scriptTask.deploymentId = runtime.getCurrentScript().deploymentId;
                var taskId = scriptTask.submit();
                log.audit('Rescheduled self', `taskId = ${taskId}`);
            } catch (e) {
                log.error('Failed to reschedule', e);
            }
        }

        return { execute };
    });
