/**
 * @NApiVersion 2.1
 * @NScriptType ScheduledScript
 */
define(['N/search', 'N/record', 'N/https', 'N/runtime', 'N/task', 'N/log', 'N/format'],
    function (search, record, https, runtime, task, log, format) {

        const MAX_PER_RUN = 40; // how many queue items to attempt per execution
        const MAX_ATTEMPTS = 12; // after this, mark ERROR and stop retrying

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
                        'custrecord_elq_record_type'
                    ]
                });

                const results = qSearch.run().getRange({ start: 0, end: MAX_PER_RUN }) || [];
                log.audit('QueueProcessor', `Found ${results.length} items`);

                // For each queue item:
                for (let i = 0; i < (results ? results.length : 0); i++) {
                    const r = results[i];
                    const qId = r.getValue('internalid');
                    const soId = parseInt(r.getValue('custrecord_elq_so_id'), 10);
                    const attempts = parseInt(r.getValue('custrecord_elq_attempts') || 0, 10);
                    const rawPayload = r.getValue('custrecord_elq_payload');
                    const context = r.getValue('custrecord_elq_context');
                    const elogiiId = r.getValue('custrecord_elq_elogii_id');
                    const recordType = r.getValue('custrecord_elq_record_type');

                    log.debug('Processing Queue Item', `QID: ${qId}, SOID: ${soId}, Attempts: ${attempts}, Context: ${context}`);

                    if (!rawPayload) {
                        markError(qId, 'Empty payload');
                        continue;
                    }

                    let parsed;
                    try { parsed = JSON.parse(rawPayload); } catch (e) { markError(qId, 'Invalid JSON payload: ' + e.message); continue; }
                    const payload = parsed.payload;

                    const creds = getElogiiApiKeyAndTaskURL();
                    const apiKey = creds.apiKey;
                    const baseUrl = creds.elogiiURL;

                    if (!apiKey || !baseUrl) {
                        markError(qId, 'Missing Elogii API credentials');
                        continue;
                    }

                    if (!apiKey || !baseUrl) {
                        markError(qId, 'Missing Elogii credentials');
                        continue;
                    }

                    try {
                        let resp;

                        // --- DELETE ---
                        if (context === 'delete') {
                            if (!elogiiId) {
                                markError(qId, 'Missing eLogii ID for delete');
                                continue;
                            }

                            const deleteUrl = `${baseUrl}?uid=${elogiiId}`;
                            recordDebugUrl(qId, deleteUrl);

                            const delResp = https.delete({
                                url: deleteUrl,
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Authorization': `ApiKey ${apiKey}`
                                }
                            });

                            if (delResp.code === 200 || delResp.code === 204) {
                                record.submitFields({
                                    type: 'customrecord_elogii_queue',
                                    id: qId,
                                    values: { custrecord_elq_status: STATUS.SUCCESS }
                                });
                                log.audit('QueueProcessor delete', `Deleted eLogii task ${elogiiId} successfully`);
                            } else if (delResp.code === 404) {
                                markError(qId, `Task not found in eLogii (404) for ${elogiiId}`);
                            } else {
                                markRetry(qId, attempts, `Delete failed - ${delResp.code}`);
                            }
                            continue;
                        }

                        // --- CREATE ---
                        if (context === 'create' || context === 'copy') {
                            recordDebugUrl(qId, baseUrl);

                            resp = https.post({
                                url: baseUrl,
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Authorization': `ApiKey ${apiKey}`
                                },
                                body: JSON.stringify(payload)
                            });
                        }

                        // --- EDIT ---
                        else if (context === 'edit') {
                            delete payload.type;
                            delete payload.date;

                            const editUrl = `${baseUrl}?uid=${elogiiId}`;
                            recordDebugUrl(qId, editUrl);

                            resp = https.put({
                                url: editUrl,
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Authorization': `ApiKey ${apiKey}`
                                },
                                body: JSON.stringify(payload)
                            });
                        }

                        // --- UNKNOWN CONTEXT ---
                        else {
                            markError(qId, `Unknown context: ${context}`);
                            continue;
                        }

                        // --- Handle response ---
                        if (!resp) {
                            markRetry(qId, attempts, 'No response');
                            continue;
                        }

                        if (resp.code === 200) {
                            let body = resp.body ? JSON.parse(resp.body) : {};
                            if (body.uid) {
                                record.submitFields({
                                    type: recordType,
                                    id: soId,
                                    values: {
                                        custbody_lap_elogii_id: String(body.uid),
                                        custbody_lap_elogii_task_status: 'Elogii Task Status: Created'
                                    }
                                });
                            }
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: qId,
                                values: { custrecord_elq_status: STATUS.SUCCESS }
                            });
                            log.audit('QueueProcessor success', `Queue ${qId} SO ${soId}`);
                        }
                        else if (resp.code >= 400 && resp.code < 500 && resp.code !== 429) {

                            log.error('Permanent failure', `Queue ${qId} failed with ${resp.code}. Marking ERROR.`);

                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: qId,
                                values: {
                                    custrecord_elq_status: STATUS.ERROR,
                                    custrecord_elq_last_error: `HTTP ${resp.code}: ${resp.body}`
                                }
                            });
                        }
                        else if (resp.code === 429) {
                            const retryAfterHeader =
                                resp.headers?.['retry-after'] || resp.headers?.['Retry-After'] || null;
                            const retrySec = retryAfterHeader
                                ? parseInt(retryAfterHeader, 10)
                                : Math.pow(2, attempts) * 60;
                            const nextRun = new Date(Date.now() + retrySec * 1000);

                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: qId,
                                values: {
                                    custrecord_elq_attempts: attempts + 1,
                                    custrecord_elq_next_run: nextRun,
                                    custrecord_elq_status: STATUS.RETRY,
                                    custrecord_elq_last_error: '429 Too Many Requests'
                                }
                            });
                            log.audit('QueueProcessor 429', `Queue ${qId} will retry after ${retrySec}s`);
                        }

                        else {
                            const errMsg = resp.body || `HTTP ${resp.code}`;
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: qId,
                                values: {
                                    custrecord_elq_attempts: attempts + 1,
                                    custrecord_elq_status:
                                        attempts + 1 >= MAX_ATTEMPTS ? STATUS.ERROR : STATUS.RETRY,
                                    custrecord_elq_last_error: String(errMsg)
                                }
                            });
                            log.error('QueueProcessor - API error', `Queue ${qId} resp ${resp.code} body: ${errMsg}`);
                        }
                    } catch (err) {
                        log.error('QueueProcessor - exception calling Elogii', err);
                        const backoff = Math.pow(2, attempts) * 60;
                        const nextRun = new Date(Date.now() + backoff * 1000);
                        record.submitFields({
                            type: 'customrecord_elogii_queue',
                            id: qId,
                            values: {
                                custrecord_elq_attempts: attempts + 1,
                                custrecord_elq_next_run: nextRun,
                                custrecord_elq_status: STATUS.RETRY,
                                custrecord_elq_last_error: String(err)
                            }
                        });
                    }
                } // for each result

                // If there might be more to do, reschedule self:
                if (results && results.length === MAX_PER_RUN) {
                    try {
                        const s = task.create({ taskType: task.TaskType.SCHEDULED_SCRIPT, scriptId: runtime.getCurrentScript().id, deploymentId: runtime.getCurrentScript().deploymentId });
                        s.submit();
                        log.audit('QueueProcessor', 'Rescheduled self (more work likely)');
                    } catch (err) {
                        log.error('QueueProcessor reschedule failed', err);
                    }
                }

            } catch (e) {
                log.error('QueueProcessor fatal', e);
            }
        }

        // helpers
        function markError(qId, msg) {
            record.submitFields({ type: 'customrecord_elogii_queue', id: qId, values: { custrecord_elq_status: STATUS.ERROR, custrecord_elq_last_error: String(msg) } });
            log.error('QueueProcessor markError', 'Queue ' + qId + ': ' + msg);
        }

        function markRetry(qId, attempts, msg) {
            const backoff = Math.pow(2, attempts) * 60;
            record.submitFields({ type: 'customrecord_elogii_queue', id: qId, values: { custrecord_elq_attempts: attempts + 1, custrecord_elq_next_run: new Date(Date.now() + backoff * 1000), custrecord_elq_status: STATUS.RETRY, custrecord_elq_last_error: msg } });
        }

        function getElogiiApiKeyAndTaskURL() {
            var obj = {};

            try {
                // Fetch Elogii-related deployment parameters from NetSuite
                const customrecord_deploymentparametersSearchObj = search.create({
                    type: "customrecord_deploymentparameters",
                    filters: [["name", "haskeywords", "Elogii"]],
                    columns: [
                        search.createColumn({ name: "name", sort: search.Sort.ASC, label: "Name" }),
                        search.createColumn({ name: "scriptid", label: "Script ID" }),
                        search.createColumn({ name: "custrecord_deploymentparametervalue", label: "Parameter Value" }),
                        search.createColumn({ name: "custrecord_deploymentparametervalue2", label: "Parameter Value 2" }),
                        search.createColumn({ name: "custrecord_parametercategory", label: "Parameter Category" }),
                        search.createColumn({ name: "custrecord_formname", label: "Form Name" }),
                        search.createColumn({ name: "custrecord_parameternotes", label: "Parameter Notes" })
                    ]
                });

                // Process each search result to extract API keys
                customrecord_deploymentparametersSearchObj.run().each(result => {
                    const deploymentParamValue1 = result.getValue({ name: 'custrecord_deploymentparametervalue' });
                    const deploymentParamValue2 = result.getValue({ name: 'custrecord_deploymentparametervalue2' });

                    // Assign API keys
                    if (deploymentParamValue1) { obj.prodAPIkey = deploymentParamValue1; }
                    if (deploymentParamValue2) { obj.sandboxAPIkey = deploymentParamValue2; }

                    return true;
                });

                const environment = runtime.envType; // Detect running environment

                // Choose appropriate API key and URL
                obj.apiKey = environment === 'SANDBOX' ? obj.sandboxAPIkey : obj.prodAPIkey;
                obj.elogiiURL = environment === 'SANDBOX' ? 'https://api-sandbox.elogii.com/tasks' : 'https://api-35.elogii.com/tasks';

                return obj;
            } catch (error) {
                log.error('Error in getElogiiApiKeyAndTaskURL function', error);
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

        return { execute };
    });