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

        // Cache to avoid duplicate customer-creation calls within a single run
        const createdCustomers = {};

        // Accumulates customers that need to be bulk-created after task processing.
        // Each entry: { customerPayload, qIds: [...], mappingItems: [...] }
        // keyed by externalId so duplicates within a run are collapsed.
        const pendingCustomerCreations = {};

        // -------------------------------------------------------------------------
        // Entry point
        // -------------------------------------------------------------------------
        function execute(context) {
            try {
                const results = fetchQueueItems();
                log.audit('QueueProcessor (batch)', `Found ${results.length} items`);
                if (!results.length) return;

                const { apiKey, elogiiURL: baseUrl } = getElogiiCredentials();
                if (!apiKey || !baseUrl) {
                    log.error('Missing credentials', 'Elogii API key or base URL missing');
                    return;
                }

                let anyRetry = false;

                // Partition queue items by operation type
                const groups = { create: [], edit: [], delete: [] };
                const mappings = { create: [], edit: [], delete: [] };

                for (const r of results) {
                    const item = extractQueueItem(r);
                    if (!item) continue;

                    const { qId, payload, ctxNorm, wrappedElogiiId, soId, elogiiInternalId, attempts } = item;

                    if (ctxNorm === 'create') {
                        const matchKey = (payload && (payload.externalId || payload.reference))
                            || (soId ? String(soId) : null)
                            || String(qId);
                        groups.create.push(payload);
                        mappings.create.push({ qId, soId, matchKey, origElogiiId: wrappedElogiiId, attempts, payload });

                    } else if (ctxNorm === 'edit') {
                        const uid = wrappedElogiiId || (payload && (payload.uid || payload.externalId)) || null;
                        const matchKey = uid || (payload && payload.externalId) || (soId ? String(soId) : null) || String(qId);
                        const editItem = Object.assign({}, payload);
                        if (uid) editItem.uid = uid;
                        groups.edit.push(editItem);
                        mappings.edit.push({ qId, soId, uid, matchKey, attempts, payload });

                    } else { // delete
                        if (elogiiInternalId) {
                            groups.delete.push(String(elogiiInternalId));
                            mappings.delete.push({ qId, soId, _id: String(elogiiInternalId), matchKey: String(elogiiInternalId), attempts });
                        } else if (wrappedElogiiId) {
                            groups.delete.push(String(wrappedElogiiId));
                            mappings.delete.push({ qId, soId, uid: String(wrappedElogiiId), matchKey: String(wrappedElogiiId), attempts });
                        } else {
                            // Nothing to delete remotely — clean up locally
                            submitQueueFields(qId, {
                                custrecord_elq_status: STATUS.SUCCESS,
                                custrecord_elq_last_error: 'No eLogii uid present for delete — cleaned up locally'
                            });
                        }
                    }
                }

                if (groups.delete.length) {
                    processBatch('delete', groups.delete, mappings.delete, baseUrl, apiKey);
                    sleep(5000); // wait 5s after removeMany before next call
                }
                if (groups.create.length) {
                    processBatch('create', groups.create, mappings.create, baseUrl, apiKey);
                    sleep(5000);
                }
                if (groups.edit.length) {
                    processBatch('edit', groups.edit, mappings.edit, baseUrl, apiKey);
                    sleep(5000);
                }

                // Collect the mappings that were held back for customer creation
                // (do this before flushing, while pendingCustomerCreations is still populated)
                const customerDependentMappings = Object.values(pendingCustomerCreations)
                    .flatMap(e => e.mappingItems);
                const customerDependentPayloads = customerDependentMappings.map(m => m.payload);

                flushPendingCustomerCreations(apiKey, baseUrl);

                // Re-run only the tasks that were blocked on missing customers
                if (customerDependentMappings.length) {
                    sleep(5000);
                    log.audit('QueueProcessor', 'Re-running create batch after customer flush');
                    processBatch('create', customerDependentPayloads, customerDependentMappings, baseUrl, apiKey);
                    sleep(5000);
                }

                // Reschedule immediately if we hit the page limit (more work likely)
                if (results.length === MAX_PER_RUN) {
                    try {
                        const s = task.create({
                            taskType: task.TaskType.SCHEDULED_SCRIPT,
                            scriptId: runtime.getCurrentScript().id,
                            deploymentId: runtime.getCurrentScript().deploymentId
                        });
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
        }

        // -------------------------------------------------------------------------
        // Queue fetch
        // -------------------------------------------------------------------------
        function fetchQueueItems() {
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
            return qSearch.run().getRange({ start: 0, end: MAX_PER_RUN }) || [];
        }

        // -------------------------------------------------------------------------
        // Parse a single search result row into a normalised queue item.
        // Returns null if the row should be skipped (e.g. empty/invalid payload).
        // -------------------------------------------------------------------------
        function extractQueueItem(r) {
            const qId = r.getValue('internalid');
            const rawPayload = r.getValue('custrecord_elq_payload') || '';
            const ctxRaw = r.getValue('custrecord_elq_context') || '';
            const elogiiId = r.getValue('custrecord_elq_elogii_id') || null;
            const soId = r.getValue('custrecord_elq_so_id') || null;
            const elogiiInternalId = r.getValue('custrecord_elq_elogii_internal_id') || null;
            const attempts = parseInt(r.getValue('custrecord_elq_attempts') || 0, 10);

            if (!rawPayload) {
                markError(qId, 'Empty payload');
                return null;
            }

            let parsed;
            try {
                parsed = JSON.parse(rawPayload);
            } catch (e) {
                // Tolerate plain-string payloads (e.g. a bare id stored previously)
                parsed = rawPayload;
            }

            // Payload may be wrapped as { payload: {...}, elogiiId: 'T-...' } or be the object itself
            const payload = (parsed && typeof parsed === 'object' && parsed.payload) ? parsed.payload : parsed;
            const wrappedElogiiId = (parsed && typeof parsed === 'object' && (parsed.elogiiId || parsed.elogiiid))
                ? (parsed.elogiiId || parsed.elogiiid)
                : (elogiiId || null);

            const ctx = String(ctxRaw).toLowerCase();
            const ctxNorm = (ctx === 'edit' || ctx === 'update') ? 'edit'
                : (ctx === 'delete') ? 'delete'
                    : 'create';

            return { qId, payload, ctxNorm, wrappedElogiiId, soId, elogiiInternalId, attempts };
        }

        // -------------------------------------------------------------------------
        // Batch processing
        // -------------------------------------------------------------------------
        function processBatch(type, itemsArray, mappingArray, baseUrl, apiKey) {
            try {
                const endpointMap = {
                    create: `${baseUrl}/createMany`,
                    edit: `${baseUrl}/updateMany`,
                    delete: `${baseUrl}/removeMany`
                };
                const url = endpointMap[type];
                if (!url) {
                    log.error('processBatch', `Unknown batch type: ${type}`);
                    return;
                }

                if (mappingArray.length > 0) {
                    recordDebugUrl(mappingArray[0].qId, url);
                }

                const bodyObj = (type === 'delete')
                    ? { ids: itemsArray }
                    : { items: itemsArray, returnItems: true };

                const postOptions = {
                    url,
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `ApiKey ${apiKey}`
                    },
                    body: JSON.stringify(bodyObj)
                };

                let resp = https.post(postOptions);

                // Retry once on 429 after a 5-second pause
                if (resp && resp.code === 429) {
                    log.audit('Batch 429 – retrying after 5s', `type=${type} count=${mappingArray.length}`);
                    sleep(5000);
                    resp = https.post(postOptions);
                }

                if (!resp) {
                    log.error('Batch no response', `type=${type} len=${itemsArray.length}`);
                    for (const m of mappingArray) markRetryGeneric(m.qId, m, 'No response from eLogii');
                    return;
                }

                const code = resp.code;
                let respBody = null;
                try {
                    respBody = resp.body ? JSON.parse(resp.body) : null;
                } catch (e) {
                    respBody = { raw: resp.body };
                }

                if (code === 429) {
                    log.audit('Batch 429 (persisted after retry)', `type=${type} count=${mappingArray.length}`);
                    for (const m of mappingArray) markRetryGeneric(m.qId, m, '429 Too Many Requests');
                    return;
                }

                if (code >= 200 && code < 300) {
                    log.audit('Batch success', `type=${type} count=${mappingArray.length}`);
                    handleBatchSuccess(type, respBody, mappingArray, apiKey, baseUrl);
                    return;
                }

                if (code >= 400 && code < 500) {
                    log.error('Batch client error', { type, code, body: resp.body });
                    const isTooManyRequests = (resp.body || '').toLowerCase().includes('exceeded the request limit');
                    for (const m of mappingArray) {
                        if (isTooManyRequests) {
                            markRetryGeneric(m.qId, m, `HTTP ${code}: Too Many Requests`);
                        } else {
                            submitQueueFields(m.qId, {
                                custrecord_elq_status: STATUS.ERROR,
                                custrecord_elq_last_error: `HTTP ${code}: ${resp.body}`
                            });
                        }
                    }
                    return;
                }

                // 5xx / other server errors
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
        }

        // -------------------------------------------------------------------------
        // 2xx response dispatch
        // -------------------------------------------------------------------------
        function handleBatchSuccess(type, respBody, mappingArray, apiKey, baseUrl) {
            if (type === 'create' && respBody && (Array.isArray(respBody.items) || Array.isArray(respBody.errors))) {
                handleCreateManyResponse(respBody, mappingArray, apiKey, baseUrl);
                return;
            }

            // edit / delete with per-item error info
            if (respBody && (Array.isArray(respBody.errors) || respBody.result || respBody.ids)) {
                const errorMap = buildErrorMap(respBody.errors);
                for (let i = 0; i < mappingArray.length; i++) {
                    const m = mappingArray[i];
                    const itemError = errorMap[i] || null;
                    if (itemError) {
                        handleItemError(type, m, itemError, apiKey, baseUrl);
                    } else {
                        submitQueueFields(m.qId, {
                            custrecord_elq_status: STATUS.SUCCESS,
                            custrecord_elq_last_error: `${type} success`
                        });
                    }
                }
                return;
            }

            // Ambiguous success — no per-item info
            for (const m of mappingArray) {
                try {
                    submitQueueFields(m.qId, {
                        custrecord_elq_status: STATUS.SUCCESS,
                        custrecord_elq_last_error: ''
                    });
                } catch (e) {
                    log.error('Batch ambiguous result update fail', { qId: m.qId, err: e });
                }
            }
        }

        // -------------------------------------------------------------------------
        // createMany response handler (three-pass: errors → successes → leftovers)
        // -------------------------------------------------------------------------
        function handleCreateManyResponse(respBody, mappingArray, apiKey, baseUrl) {
            // Index mappings by their match key for O(1) lookup
            const mapByKey = {};
            for (const m of mappingArray) {
                const key = m.matchKey || m.uid || (m.soId ? String(m.soId) : null) || String(m.qId);
                if (!mapByKey[key]) mapByKey[key] = [];
                mapByKey[key].push(m);
            }

            const errorMap = buildErrorMap(respBody.errors);

            // Pass 1 — handle per-index errors
            for (let i = 0; i < mappingArray.length; i++) {
                const m = mappingArray[i];
                const itemError = errorMap[i];
                if (!itemError) continue;

                m._handled = true;
                handleItemError('create', m, itemError, apiKey, baseUrl);
            }

            // Pass 2 — match returned items to queue records
            if (Array.isArray(respBody.items)) {
                for (const returned of respBody.items) {
                    const uid = returned.uid || returned._id || returned.externalId || returned.reference;
                    const internalId = returned._id || null;
                    const externalId = returned.externalId || null;
                    const reference = returned.reference || null;

                    const candidates = [
                        uid && String(uid),
                        externalId && String(externalId),
                        reference && String(reference),
                        reference && String(reference).toUpperCase()
                    ].filter(Boolean);

                    let matched = null;
                    for (const k of candidates) {
                        if (mapByKey[k] && mapByKey[k].length > 0) {
                            matched = mapByKey[k].shift();
                            break;
                        }
                    }

                    if (!matched) {
                        if (mappingArray.length === 1) {
                            matched = mappingArray[0];
                        } else {
                            log.audit('Unmatched returned item', { returned });
                            continue;
                        }
                    }

                    matched._handled = true;

                    try {
                        submitQueueFields(matched.qId, {
                            custrecord_elq_elogii_id: String(uid || externalId || internalId),
                            custrecord_elq_elogii_internal_id: String(internalId || ''),
                            custrecord_elq_status: STATUS.SUCCESS,
                            custrecord_elq_last_error: ''
                        });
                    } catch (e) {
                        log.error('Failed to update queue record for returned item', { qId: matched.qId, err: e });
                    }

                    if (matched.soId) {
                        updateOriginatingTransaction(matched.soId, uid, internalId, reference);
                    }
                }
            }

            // Pass 3 — anything still unhandled is an implicit error
            for (const m of mappingArray) {
                if (m._handled) continue;
                try {
                    submitQueueFields(m.qId, {
                        custrecord_elq_status: STATUS.ERROR,
                        custrecord_elq_last_error: 'createMany did not return item and no explicit error received'
                    });
                } catch (e) {
                    log.error('Failed marking leftover mapping as ERROR', { qId: m.qId, err: e });
                }
                log.warn('Unprocessed createMany item', { qId: m.qId });
            }
        }

        // -------------------------------------------------------------------------
        // Per-item error handler (shared by create, edit, delete paths)
        // -------------------------------------------------------------------------
        function handleItemError(type, m, itemError, apiKey, baseUrl) {
            const errMsg = JSON.stringify(itemError);

            if (errMsg.includes('Customer externalId') && errMsg.includes('cannot be found')) {
                // Queue this customer for bulk creation at the end of the run.
                // payload.customer.internalId becomes eLogii's externalId (the lookup key);
                // payload.customer.externalId (customerId + customerText) becomes the display name.
                const customer = m.payload && m.payload.customer;
                const elogiiExtId = customer && customer.externalId;
                const elogiiName = m.payload.location && m.payload.location.name;
                const custId = elogiiName.split(" ")[0];
                if (elogiiExtId) {
                    if (!pendingCustomerCreations[elogiiExtId]) {
                        const customerPayload = {
                            externalId: elogiiExtId,
                            name: elogiiName || elogiiExtId,
                            reference: custId
                        };
                        pendingCustomerCreations[elogiiExtId] = {
                            customerPayload,
                            mappingItems: []
                        };
                    }
                    pendingCustomerCreations[elogiiExtId].mappingItems.push(m);
                    log.audit('Queued customer for bulk creation', { elogiiExtId, qId: m.qId });
                } else {
                    markError(m.qId, `Customer not found and no internalId available: ${errMsg}`);
                }
            } else if (errMsg.toLowerCase().includes('exceeded the request limit')) {
                markRetryGeneric(m.qId, m, `batch ${type} item error (rate limited): ${errMsg}`);
            } else {
                markError(m.qId, `batch ${type} item error: ${errMsg}`);
            }
        }

        // -------------------------------------------------------------------------
        // Update the originating SO/RMA with the eLogii UID from a successful create
        // -------------------------------------------------------------------------
        function updateOriginatingTransaction(soId, uid, internalId, reference) {
            const soIdInt = parseInt(soId, 10);
            const ref = String(reference || '').toUpperCase();
            const recordType = ref.startsWith('RMA') ? record.Type.RETURN_AUTHORIZATION : record.Type.SALES_ORDER;

            try {
                record.submitFields({
                    type: recordType,
                    id: soIdInt,
                    values: {
                        custbody_lap_elogii_id: String(uid),
                        custbody_elogii_internal_id: String(internalId || ''),
                        custbody_lap_elogii_trck_link: `https://lapwing.dash-beta.elogii.com/#/tracking?uid=${uid}`,
                        custbody_lap_elogii_task_status: 'Elogii Task Created'
                    },
                    options: { enableSourcing: false, ignoreMandatoryFields: true, disableTriggers: true }
                });
            } catch (e) {
                log.error('Could not update originating transaction with UID', { soId, err: e });
            }
        }

        // -------------------------------------------------------------------------
        // Helpers
        // -------------------------------------------------------------------------

        /** Build an index → error object map from an errors array. */
        function buildErrorMap(errors) {
            const errorMap = {};
            if (Array.isArray(errors)) {
                for (const e of errors) {
                    if (typeof e.index !== 'undefined') errorMap[e.index] = e;
                }
            }
            return errorMap;
        }

        /** Thin wrapper around record.submitFields for queue records. */
        function submitQueueFields(qId, values) {
            record.submitFields({ type: 'customrecord_elogii_queue', id: qId, values });
        }

        function markRetryGeneric(qId, mappingItem, msg) {
            try {
                const attempts = parseInt(mappingItem.attempts || 0, 10) + 1;
                const backoffSec = Math.pow(2, attempts) * 60;
                const nextRun = new Date(Date.now() + backoffSec * 1000);
                submitQueueFields(qId, {
                    custrecord_elq_attempts: attempts,
                    custrecord_elq_next_run: nextRun,
                    custrecord_elq_status: STATUS.RETRY,
                    custrecord_elq_last_error: String(msg)
                });
            } catch (e) {
                log.error('markRetryGeneric failed', { qId, err: e });
            }
        }

        function markError(qId, msg) {
            try {
                submitQueueFields(qId, {
                    custrecord_elq_status: STATUS.ERROR,
                    custrecord_elq_last_error: String(msg)
                });
            } catch (e) {
                log.error('markError failed', { qId, err: e });
            }
        }

        function recordDebugUrl(qId, urlString) {
            try {
                submitQueueFields(qId, { custrecord_elq_debug_url: `DEBUG URL: ${urlString}` });
            } catch (e) {
                log.error('Error saving debug URL', e);
            }
        }

        function getElogiiCredentials() {
            const obj = {};
            try {
                search.create({
                    type: 'customrecord_deploymentparameters',
                    filters: [['name', 'haskeywords', 'Elogii']],
                    columns: [
                        search.createColumn({ name: 'custrecord_deploymentparametervalue' }),
                        search.createColumn({ name: 'custrecord_deploymentparametervalue2' })
                    ]
                }).run().each(result => {
                    const v1 = result.getValue({ name: 'custrecord_deploymentparametervalue' });
                    const v2 = result.getValue({ name: 'custrecord_deploymentparametervalue2' });
                    if (v1) obj.prodAPIkey = v1;
                    if (v2) obj.sandboxAPIkey = v2;
                    return true;
                });

                const isSandbox = runtime.envType === 'SANDBOX';
                obj.apiKey = isSandbox ? obj.sandboxAPIkey : obj.prodAPIkey;
                obj.elogiiURL = isSandbox ? 'https://api-sandbox.elogii.com/tasks' : 'https://api-35.elogii.com/tasks';
            } catch (err) {
                log.error('getElogiiCredentials error', err);
            }
            return obj;
        }

        function rescheduleIfNeeded(anyRetryFlag) {
            if (!anyRetryFlag) return;
            try {
                const scriptTask = task.create({ taskType: task.TaskType.SCHEDULED_SCRIPT });
                scriptTask.scriptId = runtime.getCurrentScript().id;
                scriptTask.deploymentId = runtime.getCurrentScript().deploymentId;
                const taskId = scriptTask.submit();
                log.audit('Rescheduled self', `taskId = ${taskId}`);
            } catch (e) {
                log.error('Failed to reschedule', e);
            }
        }

        /**
         * Sends a single customers/createMany request for every customer that was
         * collected into pendingCustomerCreations during this run, then marks the
         * originating queue items for retry so they are picked up next time.
         */
        function flushPendingCustomerCreations(apiKey, baseUrl) {
            const entries = Object.values(pendingCustomerCreations);
            if (!entries.length) return;

            const customersUrl = baseUrl.replace('/tasks', '/customers/createMany');
            const items = entries.map(e => e.customerPayload);

            log.audit('flushPendingCustomerCreations', `Sending createMany for ${items.length} customer(s)`);

            const customerPostOptions = {
                url: customersUrl,
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `ApiKey ${apiKey}`
                },
                body: JSON.stringify({ items })
            };

            let resp;
            try {
                resp = https.post(customerPostOptions);

                // Retry once on 429 after a 5-second pause
                if (resp && resp.code === 429) {
                    log.audit('flushPendingCustomerCreations 429 – retrying after 5s', `count=${items.length}`);
                    sleep(5000);
                    resp = https.post(customerPostOptions);
                }
            } catch (e) {
                log.error('flushPendingCustomerCreations HTTP error', e);
                // Mark all dependant queue items for retry so they are not lost
                for (const entry of entries) {
                    for (const m of entry.mappingItems) {
                        markRetryGeneric(m.qId, m, 'customers/createMany HTTP exception: ' + String(e));
                    }
                }
                return;
            }

            const code = resp.code;
            const respText = resp.body || '';
            let respBody = null;
            try { respBody = JSON.parse(respText); } catch (_) { respBody = { raw: respText }; }

            if (code === 429 || (respText.toLowerCase().includes('exceeded the request limit'))) {
                log.audit('flushPendingCustomerCreations 429 (persisted after retry)', 'Will retry task items next run');
                for (const entry of entries) {
                    for (const m of entry.mappingItems) {
                        markRetryGeneric(m.qId, m, 'customers/createMany rate limited (429)');
                    }
                }
                return;
            }

            if (code >= 200 && code < 300) {
                log.audit('flushPendingCustomerCreations success', `HTTP ${code}`);

                // Build a set of successfully created / already-existing externalIds
                const confirmed = new Set();

                // The createMany response may report per-item errors; anything not in
                // the errors array (or whose error says "already exists") is a success.
                const errorsByIndex = buildErrorMap(respBody && respBody.errors);

                entries.forEach((entry, idx) => {
                    const itemErr = errorsByIndex[idx];
                    const externalId = entry.customerPayload.externalId;

                    if (!itemErr || JSON.stringify(itemErr).toLowerCase().includes('already exists')) {
                        confirmed.add(externalId);
                        createdCustomers[externalId] = true;
                        log.audit('Customer confirmed', { externalId });
                    } else {
                        log.error('Customer createMany item error', { externalId, err: itemErr });
                    }
                });

                // Mark queue items: retry if customer confirmed, error otherwise
                for (const entry of entries) {
                    const externalId = entry.customerPayload.externalId;
                    const wasCreated = confirmed.has(externalId);
                    for (const m of entry.mappingItems) {
                        if (wasCreated) {
                            markRetryGeneric(m.qId, m, 'Customer created via createMany — retrying task');
                        } else {
                            markError(m.qId, `customers/createMany failed for externalId ${externalId}`);
                        }
                    }
                }
                return;
            }

            // Any other non-2xx
            log.error('flushPendingCustomerCreations failed', { code, body: respText });
            for (const entry of entries) {
                for (const m of entry.mappingItems) {
                    markRetryGeneric(m.qId, m, `customers/createMany HTTP ${code}`);
                }
            }
        }

        function sleep(ms) {
            const start = Date.now();
            while (Date.now() - start < ms) { /* busy-wait */ }
        }

        return { execute };
    });
