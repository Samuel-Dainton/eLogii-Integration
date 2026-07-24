/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/record', 'N/search', 'N/runtime', 'N/log', 'N/format', 'N/task', 'N/query'],
    function (record, search, runtime, log, format, task, query) {

        const STATUS = {
            PENDING: 1,
            RETRY: 2,
            SUCCESS: 3,
            ERROR: 4,
            PROCESSING: 5,
            PROCESSED: 6
        };

        function getInputData() {
            log.debug('getInputData', 'Searching for PENDING queue records');
            return search.create({
                type: 'customrecord_elogii_queue',
                filters: [
                    ['custrecord_elq_status', 'anyof', STATUS.PENDING]
                ],
                columns: [
                    'internalid'
                ]
            });
        }

        // function map(context) {
        //     const queueId = context.value;

        //     log.debug('map', `Processing queue ID ${queueId}`);

        //     context.write({
        //         key: queueId,
        //         value: queueId
        //     });
        // }

        function reduce(context) {
            const queueId = parseInt(context.key, 10);

            try {
                // --- Lookup queue record fields via N/query (avoids record.load governance) ---
                let soIdRaw, queueContext, recordType, elogiiId, elogiiInternalId;
                try {
                    const queueResults = query.runSuiteQL({
                        query: `
                            SELECT
                                custrecord_elq_so_id,
                                custrecord_elq_context,
                                custrecord_elq_record_type,
                                custrecord_elq_elogii_id,
                                custrecord_elq_elogii_internal_id
                            FROM customrecord_elogii_queue
                            WHERE id = ?
                        `,
                        params: [queueId]
                    }).asMappedResults();

                    if (!queueResults || queueResults.length === 0) {
                        log.error('Reduce - Queue Lookup Failed', `Queue ${queueId} not found via SuiteQL`);
                        return;
                    }

                    const qRow = queueResults[0];
                    soIdRaw = qRow.custrecord_elq_so_id;
                    queueContext = qRow.custrecord_elq_context;     // create / edit / delete / backorder
                    recordType = qRow.custrecord_elq_record_type || record.Type.SALES_ORDER;
                    elogiiId = qRow.custrecord_elq_elogii_id || null;
                    elogiiInternalId = qRow.custrecord_elq_elogii_internal_id || null;
                } catch (e) {
                    log.error('Reduce - Queue Lookup Failed', `Queue ${queueId} SuiteQL error: ${e.message}`);
                    return;
                }

                // // Mark PROCESSING early so other MR runs / SS know it's being handled
                // try {
                //     record.submitFields({
                //         type: 'customrecord_elogii_queue',
                //         id: queueId,
                //         values: { custrecord_elq_status: STATUS.PROCESSING }
                //     });
                // } catch (e) {
                //     log.error('Reduce - Mark Processing Failed', `Queue ${queueId} cannot be set to PROCESSING: ${e.message}`);
                // }

                // ensure soId is integer string (clean any accidental decimals)
                const soId = parseInt(String(soIdRaw).replace(/\D/g, ''), 10);
                log.debug('Reduce - Start', `Queue ${queueId} for ${recordType} ${soId} (context=${queueContext})`);

                // --- Consolidation: keep only the most recent PENDING/PROCESSING queue for this SO ---
                try {
                    const consolidationSearch = search.create({
                        type: 'customrecord_elogii_queue',
                        filters: [
                            ['custrecord_elq_so_id', 'is', soId],
                            'AND',
                            ['custrecord_elq_status', 'anyof', [STATUS.PENDING, STATUS.PROCESSING, STATUS.PROCESSED, STATUS.RETRY]]
                        ],
                        columns: [
                            search.createColumn({ name: 'internalid', sort: search.Sort.DESC }),
                            search.createColumn({ name: 'lastmodified', sort: search.Sort.DESC })
                        ]
                    });

                    const rows = consolidationSearch.run().getRange({ start: 0, end: 1000 }) || [];

                    if (rows.length > 1) {
                        const mostRecentId = rows[0].getValue('internalid');
                        for (let i = 1; i < rows.length; i++) {
                            const oldId = rows[i].getValue('internalid');
                            try {
                                record.delete({ type: 'customrecord_elogii_queue', id: oldId });
                                log.audit('Queue Consolidation', `Deleted older queue ${oldId} for SO ${soId}`);
                            } catch (e) {
                                log.error('Queue Consolidation Error', `Could not delete queue ${oldId}: ${e.message}`);
                            }
                        }

                        if (String(queueId) !== String(mostRecentId)) {
                            log.audit('Queue Consolidation', `Skipping queue ${queueId} because newer queue ${mostRecentId} exists for SO ${soId}`);
                            return; // another (more recent) queue will handle processing
                        }
                    }
                } catch (e) {
                    log.error('Consolidation Search Error', e);
                }

                // --- Lookup all required transaction body fields via N/query ---
                let txnData = null;
                let soStatus = null;

                if (queueContext !== 'delete') {
                    try {
                        const txnResults = query.runSuiteQL({
                            query: `
                                SELECT
                                    t.status,
                                    t.custbody_lap_elogii_id,
                                    t.custbody_elogii_id_hist,
                                    t.trandate,
                                    t.shipdate,
                                    t.tranid,
                                    t.custbody_stc_amount_after_discount,
                                    t.memo,
                                    t.custbody_daterequired,
                                    t.entity,
                                    CASE
                                        WHEN c.isperson = 'T' THEN c.firstname || ' ' || c.lastname
                                        ELSE c.companyname
                                    END AS entityname,
                                    c.entityid AS entityid,
                                    t.custbody_fulfillment_email,
                                    t.custbody_customer_band,
                                    BUILTIN.DF(t.custbody_customer_band) AS customerbandname,
                                    t.custbody_lpl_sitecontact,
                                    t.custbody_lpl_sitecontactphone,
                                    t.custbody_alf_subsidiary_name AS subsidiaryname,
                                    t.shipmethod,
                                    sm.itemid AS shipmethodname,
                                    t.custbody_lap_cust_pickup,
                                    t.custbody_lap_deliv_by_time,
                                    t.custbody_raisedby,
                                    t.custbody_estimatedgrossprofit,
                                    t.custbody_estimatedgrossprofitpercent,
                                    e.firstname || ' ' || e.lastname AS raisedbyname,
                                    sa.addr1     AS shipaddr1,
                                    sa.addr2     AS shipaddr2,
                                    sa.city      AS shipcity,
                                    sa.zip       AS shipzip,
                                    sa.country   AS shipcountry
                                FROM transaction t
                                LEFT JOIN customer c ON c.id = t.entity
                                LEFT JOIN employee e ON e.id = t.custbody_raisedby
                                LEFT JOIN shipItem sm ON sm.id = t.shipmethod
                                LEFT JOIN transactionShippingAddress sa
                                    ON sa.nkey = t.shippingaddress
                                WHERE t.id = ?
                            `,
                            params: [soId]
                        }).asMappedResults();

                        if (!txnResults || txnResults.length === 0) {
                            throw new Error(`Transaction ${soId} not found via SuiteQL`);
                        }

                        txnData = txnResults[0];
                        soStatus = txnData.status || null;

                        log.debug('Reduce - Transaction Lookup', `${recordType} ${soId} queried; status=${soStatus}`);

                        // --- SAFETY CHECK: SO now has an eLogii ID but queue record does not ---
                        const currentElogiiId = txnData.custbody_lap_elogii_id || null;
                        if (currentElogiiId && !elogiiId) {
                            log.audit('Elogii ID Sync',
                                `Queue ${queueId} missing eLogii ID. SO ${soId} has ID ${currentElogiiId}. Updating queue.`);

                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: queueId,
                                values: {
                                    custrecord_elq_elogii_id: currentElogiiId,
                                    custrecord_elq_context: 'edit'
                                }
                            });

                            elogiiId = currentElogiiId;
                            queueContext = 'edit';
                        }

                    } catch (e) {
                        log.error('Transaction Lookup Error', `Cannot query ${recordType} ${soId}: ${e.message}`);
                        try {
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: queueId,
                                values: {
                                    custrecord_elq_status: STATUS.PROCESSED,
                                    custrecord_elq_last_error: `Transaction lookup failed: ${e.message}`
                                }
                            });
                        } catch (sfe) {
                            log.error('Queue Update Error', `Failed to mark queue ${queueId} PROCESSED after lookup failure: ${sfe.message}`);
                        }
                        return;
                    }
                }

                // --- Special handling for backorder context: reset fields on the transaction ---
                // Uses submitFields — no record.load required; history computed from queried fields above.
                if (queueContext === 'backorder' && txnData) {
                    try {
                        const thisElogiiId = txnData.custbody_lap_elogii_id || '';
                        const histElogiiId = txnData.custbody_elogii_id_hist || '';
                        const concatElogiiId = (thisElogiiId ? thisElogiiId : '') + (histElogiiId ? ', ' + histElogiiId : '');

                        record.submitFields({
                            type: recordType,
                            id: soId,
                            values: {
                                custbody_elogii_id_hist: concatElogiiId || histElogiiId,
                                custbody_lap_elogii_id: '',
                                custbody_lap_elogii_task_status: '',
                                custbody_lap_elogii_trck_link: '',
                                custbody_driver: '',
                                custbody_route_stop_num: '',
                                custbody_released: false
                            }
                        });
                        log.audit('Backorder Reset', `SO ${soId} fields reset for backorder via submitFields`);

                    } catch (e) {
                        log.error('Backorder Reset Error', e);
                    }
                }

                // --- Delete / Close Payload ---
                let payloadObj = null;

                // CASE 1: Hard DELETE (queueContext = 'delete')
                // Do NOT reference soRec or status here
                try {
                    if (queueContext === 'delete') {

                        if (!elogiiId) {
                            // No eLogii ID = nothing to delete on eLogii, mark success & exit
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: queueId,
                                values: {
                                    custrecord_elq_context: 'delete',
                                    custrecord_elq_status: STATUS.SUCCESS,
                                    custrecord_elq_last_error: ''
                                }
                            });
                            return;
                        }

                        // Has eLogii ID = send delete request
                        record.submitFields({
                            type: 'customrecord_elogii_queue',
                            id: queueId,
                            values: {
                                custrecord_elq_payload: elogiiInternalId,
                                custrecord_elq_context: 'delete',
                                custrecord_elq_status: STATUS.PROCESSED,
                                custrecord_elq_last_error: ''
                            }
                        });

                        return;
                    }

                    // --------------------------------------------------
                    // CASE 2: CLOSED (record exists)
                    // --------------------------------------------------

                    if (soStatus === 'Closed') {

                        // Only treat as delete if ALL lines are closed
                        const isFullyClosed = allLinesClosed(soId);
                        log.debug('Closed Status Check', `SO ${soId} closed status; fully closed = ${isFullyClosed}`);

                        if (!isFullyClosed) {
                            log.audit('Closed but Not Fully Closed',
                                `SO ${soId} has closed status but not all lines are closed — skipping delete.`);
                            return;
                        }

                        // Fully closed - now handle eLogii delete flow
                        if (!elogiiId) {
                            // No eLogii ID = nothing to delete, mark queue as success
                            log.debug('Closed without Elogii ID', `SO ${soId} closed but has no eLogii ID; marking queue ${queueId} as SUCCESS.`);
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: queueId,
                                values: {
                                    custrecord_elq_context: 'delete',
                                    custrecord_elq_status: STATUS.SUCCESS,
                                    custrecord_elq_last_error: ''
                                }
                            });
                            return;
                        } else {
                            log.debug('Closed with Elogii ID', `SO ${soId} is fully closed and has eLogii ID ${elogiiId}; enqueuing delete for internal id ${elogiiInternalId}.`);
                            // Record fully closed and has an eLogii ID = enqueue delete
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: queueId,
                                values: {
                                    custrecord_elq_payload: elogiiInternalId,
                                    custrecord_elq_context: 'delete',
                                    custrecord_elq_status: STATUS.PROCESSED,
                                    custrecord_elq_last_error: ''
                                }
                            });

                            return;
                        }
                    }
                }
                catch (e) {
                    log.error('Closed/Delete Payload Error', `Failed to build delete payload for SO ${soId}: ${e.message}`);
                    // mark queue PROCESSED with message so SS can decide
                }

                try {
                    const buildResult = buildPayloadFromSalesOrder(txnData, soId, queueContext);
                    // buildResult must return { payload, elogiiId }
                    payloadObj = buildResult && buildResult.payload ? buildResult.payload : null;
                } catch (e) {
                    log.error('Payload Build Error', `Failed to build payload for ${recordType} ${soId}: ${e.message}`);
                    // mark queue PROCESSED with message so SS can decide
                    record.submitFields({
                        type: 'customrecord_elogii_queue',
                        id: queueId,
                        values: {
                            custrecord_elq_status: STATUS.PROCESSED,
                            custrecord_elq_last_error: `Payload build failed: ${e.message}`
                        }
                    });
                    return;
                }

                // --- Write payload back to queue record and mark PROCESSED ---
                try {
                    const valuesToSet = {
                        custrecord_elq_last_error: ''
                    };

                    if (payloadObj !== null) {
                        valuesToSet.custrecord_elq_payload = JSON.stringify({ payload: payloadObj, elogiiId: elogiiId });
                    } else {
                        // clear payload if delete or none
                        valuesToSet.custrecord_elq_payload = '';
                    }

                    // do not overwrite custrecord_elq_context (SS uses this), but if backorder we want SS to treat as create:
                    if (queueContext === 'backorder') {
                        valuesToSet.custrecord_elq_context = 'create'; // override so SS knows to create
                    }

                    valuesToSet.custrecord_elq_status = STATUS.PROCESSED;

                    record.submitFields({
                        type: 'customrecord_elogii_queue',
                        id: queueId,
                        values: valuesToSet
                    });

                    log.audit('Reduce - Completed', `Queue ${queueId} for SO ${soId} marked PROCESSED`);
                } catch (e) {
                    log.error('Queue Update Error', `Failed to update queue ${queueId}: ${e.message}`);
                    // Best effort: mark PROCESSED with error message so SS can examine
                    try {
                        record.submitFields({
                            type: 'customrecord_elogii_queue',
                            id: queueId,
                            values: {
                                custrecord_elq_status: STATUS.PROCESSED,
                                custrecord_elq_last_error: `Update failed: ${e.message}`
                            }
                        });
                    } catch (ee) {
                        log.error('Queue Update Fallback Error', ee);
                    }
                }

            } catch (err) {
                log.error('Reduce - General Error', err);
                // Ensure MR doesn't leave the queue in a bad state without a note
                try {
                    record.submitFields({
                        type: 'customrecord_elogii_queue',
                        id: queueId,
                        values: {
                            custrecord_elq_status: STATUS.PROCESSED,
                            custrecord_elq_last_error: `MR failure: ${err.message}`
                        }
                    });
                } catch (ee) {
                    log.error('Reduce - Failed to write fallback', ee);
                }
            }
        }

        function summarize(summary) {
            try {
                const ssTask = task.create({
                    taskType: task.TaskType.SCHEDULED_SCRIPT,
                    scriptId: 'customscript_elogii_queue_processor',
                    deploymentId: 'customdeploy_elogii_queue_processor'
                });

                const taskId = ssTask.submit();
                log.audit("SS Triggered", `Queue Processor Task ID: ${taskId}`);

            } catch (e) {
                log.error("SS Trigger Error", e);
            }
            if (summary.inputSummary.error) log.error('Input error', summary.inputSummary.error);
            summary.mapSummary.errors.iterator().each((key, err) => { log.error('Map error ' + key, err); return true; });
            summary.reduceSummary.errors.iterator().each((key, err) => { log.error('Reduce error ' + key, err); return true; });
        }

        // ---------------- Helpers ----------------

        // ---------- helper to build payload ----------
        /**
         * Builds the eLogii payload from a plain txnData object returned by N/query.
         * @param {Object} txnData  - Mapped SuiteQL result row for the transaction.
         * @param {number} soId     - Internal ID of the transaction.
         */
        function buildPayloadFromSalesOrder(txnData, soId, queueContext) {

            const parseNSDate = (value) => {
                if (!value) return null;

                const parts = String(value).split('/');

                return parts.length === 3
                    ? new Date(parts[2], parts[1] - 1, parts[0]) // DD/MM/YYYY
                    : new Date(value);
            };

            const todayNow = new Date();

            const today = new Date(
                todayNow.getFullYear(),
                todayNow.getMonth(),
                todayNow.getDate()
            );

            let shipDate = parseNSDate(txnData.shipdate)
            const reqDate = parseNSDate(txnData.custbody_daterequired);

            if (queueContext === 'create') {
                log.audit("Overwrite Date", shipDate + " with " + today)
                shipDate = today;
            }

            const futureOrder =
                reqDate && reqDate > today
                    ? 'Future Order'
                    : undefined;

            if (reqDate && (!shipDate || reqDate > shipDate)) {
                shipDate = reqDate;
            }

            if (!shipDate || shipDate < today) {
                shipDate = today;
            }

            const yyyy = shipDate.getFullYear();
            const mm = String(shipDate.getMonth() + 1).padStart(2, '0');
            const dd = String(shipDate.getDate()).padStart(2, '0');

            const formattedShipDate = Number(`${yyyy}${mm}${dd}`);

            const tranDocNumber = txnData.tranid;
            const subTotal = txnData.custbody_stc_amount_after_discount;
            const memo = txnData.memo;
            const elogiiId = txnData.custbody_lap_elogii_id || null;

            // Customer + contact
            const customerText = txnData.entityname || '';  // resolved display name from customer join
            const customerId = String(txnData.entityid || '');
            const customerInternalId = String(txnData.entity);
            const customerItemFulfilEmail = txnData.custbody_fulfillment_email || null;
            const spendBand = txnData.custbody_customer_band;
            const siteContactName = txnData.custbody_lpl_sitecontact;
            const siteContactPhoneNum = txnData.custbody_lpl_sitecontactphone;

            // Subsidiary details (cached after first lookup)
            const subsName = txnData.subsidiaryname;
            const subsObj = getSubsidiary(subsName);

            // Shipping address — available directly on the transaction table
            const shipaddr1 = txnData.shipaddr1 || '';
            const shipaddr2 = txnData.shipaddr2 || '';
            const shipcity = txnData.shipcity || '';
            const shipzip = txnData.shipzip || '';
            const shipcountry = txnData.shipcountry || '';

            const shipMethod = txnData.shipmethodname || '';
            let collection = txnData.custbody_lap_cust_pickup;
            collection = (collection === 'T' || collection === true) ? 'Collection' : null;

            let deliveryService = txnData.custbody_lap_deliv_by_time ? String(txnData.custbody_lap_deliv_by_time) : null;
            if (deliveryService !== '10:30' && deliveryService !== '12:00') {
                deliveryService = null;
            }

            const spendBandStr = txnData.customerbandname || null;

            // Build skills array
            const skills = [shipMethod, deliveryService, futureOrder, spendBandStr, collection].filter(Boolean);

            // Line items via SuiteQL (no record object needed)
            const lineItems = getSOLinesArr(soId);
            log.debug('buildPayloadFromSalesOrder - lineItems count', lineItems ? lineItems.length : 'null/undefined');

            // -----------------
            // Build eLogii payload
            // -----------------
            const payload = {
                externalId: String(soId),
                reference: tranDocNumber,
                type: 1,
                date: formattedShipDate,
                orderValue: subTotal,
                skills: skills,
                pickup: {
                    location: {
                        type: 2,
                        name: subsObj.addressee,
                        address: subsObj.addr1,
                        addressLine2: subsObj.addr2,
                        postCode: subsObj.zip,
                        city: subsObj.city,
                        country: subsObj.country,
                        contactName: txnData.raisedbyname || '',
                        contactPhone: subsObj.addrphone
                    },
                    // instructions: txnData.custbody_drivernotes
                },
                customer: {
                    externalId: customerInternalId,
                },
                location: {
                    type: 2,
                    name: customerId + ' ' + customerText,
                    address: `${shipaddr1 || ""} ${shipaddr2 || ""} ${shipcity || ""} ${shipzip || ""} ${shipcountry || ""}`.trim(),
                    addressLine2: shipaddr2,
                    city: shipcity,
                    country: shipcountry,
                    postCode: shipzip,
                    contactName: siteContactName,
                    contactPhone: siteContactPhoneNum,
                    contactEmail: customerItemFulfilEmail
                },
                items: lineItems,
                internalComment: memo,
                customData: {
                    RequiredDate: reqDate,
                    EstimatedGrossProfit: `£${txnData.custbody_estimatedgrossprofit || 0}`,
                    EstimatedGrossProfitPercent: `%${txnData.custbody_estimatedgrossprofitpercent || 0}`
                }
            };

            // // If Return Authorization = swap pickup and dropoff locations
            // if (recordType === 'returnauthorization' || recordType === record.Type.RETURN_AUTHORIZATION) {
            //     const originalPickup  = payload.pickup.location;
            //     const originalLocation = payload.location;
            //     payload.pickup.location = originalLocation;
            //     payload.location = originalPickup;
            //     log.debug('RMA Payload Adjustment', 'Swapped pickup and location for return authorization');
            // }

            return { payload, elogiiId };
        }

        /**
         * Fetches SO line items via SuiteQL.
         * Joins transaction -> orderLine -> transactionLine -> item.
         * orderLine is the correct SuiteQL table for SO lines and contains custcol_quantityremaining
         * via its join to transactionLine.
         * @param {number} soId - Internal ID of the transaction.
         * @returns {Array} Array of item objects for the eLogii payload.
         */
        const getSOLinesArr = (soId) => {
            try {
                const lineResults = query.runSuiteQL({
                    query: `
                        SELECT
                            tl.item,
                            BUILTIN.DF(tl.item)          AS itemname,
                            ABS(tl.quantity)             AS quantity,
                            ABS(tl.quantityShipRecv)     AS quantityShipRecv,
                            tl.memo                      AS description,
                            tl.custcol_ci_itemweight     AS itemweight,
                            i.weight                     AS itemweight2 
                        FROM transaction t
                        INNER JOIN transactionLine tl
                            ON tl.transaction = t.id
                        LEFT JOIN item i
                            ON i.id = tl.item
                        WHERE t.id = ?
                          AND t.type = 'SalesOrd'
                          AND tl.item IS NOT NULL
                          AND tl.mainline = 'F'
                          AND tl.taxline = 'F'
                    `,
                    params: [soId]
                }).asMappedResults();

                log.debug('getSOLinesArr - raw results', JSON.stringify(lineResults));

                const salesOrderItemsArr = [];

                for (const row of lineResults) {
                    let description = row.description;
                    if (!description) description = 'Missing Description';

                    const totalQty = parseFloat(row.quantity) || 0;
                    const quantityshiprecv = parseFloat(row.quantityshiprecv);
                    // quantityShipRecv may be null before any fulfilment — fall back to total qty
                    const remainingQty = totalQty - quantityshiprecv;
                    const quantity = remainingQty > 0 ? remainingQty : 0;

                    const itemDisplay = row.itemname || String(row.item);
                    let weight = parseFloat(row.itemweight2);
                    if (!weight || isNaN(weight)) weight = 0.1;

                    log.debug('getSOLinesArr - row', JSON.stringify({ itemDisplay, quantity, totalQty, remainingQty, weight }));

                    if (quantity > 0) {
                        salesOrderItemsArr.push({
                            description,
                            state: 0,
                            customData: { qty: quantity, itemDisplay },
                            quantity,
                            unitSize: { "Weight kg": weight }
                        });
                    }
                }

                log.debug('getSOLinesArr - final array', JSON.stringify(salesOrderItemsArr));
                return salesOrderItemsArr;
            } catch (error) {
                log.error('Error in getSOLinesArr function', error);
                return [];
            }
        };

        /**
         * Fetches details about a subsidiary based on its ID.
         * @param {string} subsId - Subsidiary ID.
         * @returns {Object} - An object containing various subsidiary details.
         */
        const subsidiaryCache = new Map();

        let getSubsidiary = (subsName) => {
            if (subsidiaryCache.has(subsName)) return subsidiaryCache.get(subsName);

            if (!subsName) {
                // Handle the error appropriately
                throw new Error("subsName must be provided");
            }

            let subObj = {};
            try {
                // Use N/query to fetch subsidiary main address fields directly — avoids record.load governance hit
                const subsResults = query.runSuiteQL({
                    query: `
                        SELECT
                            s.id,
                            a.addr1,
                            a.addr2,
                            a.city,
                            a.country,
                            a.addrphone,
                            a.zip,
                            a.addressee
                        FROM subsidiary s
                        LEFT JOIN SubsidiaryMainAddress a ON a.nkey = s.mainAddress
                        WHERE s.name = ?
                    `,
                    params: [subsName]
                }).asMappedResults();

                if (!subsResults || subsResults.length === 0) {
                    throw new Error(`Subsidiary '${subsName}' not found via SuiteQL`);
                }

                const row = subsResults[0];
                subObj = {
                    addr1: row.addr1 || '',
                    addr2: row.addr2 || '',
                    city: row.city || '',
                    country: row.country || '',
                    addrphone: row.addrphone || ' ',
                    zip: row.zip || '',
                    addressee: row.addressee || ''
                };

                subsidiaryCache.set(subsName, subObj);
                return subObj;

            } catch (error) { log.error('error in getSubsidiary function', error); }
        }

        /**
         * Returns true only when every line on the transaction has zero remaining quantity.
         * Uses SuiteQL on transactionline — no record.load required.
         * @param {number} soId - Internal ID of the transaction.
         */
        function allLinesClosed(soId) {
            try {
                const result = query.runSuiteQL({
                    query: `
                        SELECT COUNT(*) AS openLines
                        FROM transactionline
                        WHERE transaction = ?
                          AND itemtype  != 'Description'
                          AND item IS NOT NULL
                          AND (quantity - quantityfulfilled) > 0
                    `,
                    params: [soId]
                }).asMappedResults();

                const openLines = result && result[0] ? parseInt(result[0].openlines, 10) : 0;
                return openLines === 0;
            } catch (e) {
                log.error('allLinesClosed Error', e);
                return false; // fail safe: don't treat as closed if query fails
            }
        }

        return { getInputData, reduce, summarize };
    });
