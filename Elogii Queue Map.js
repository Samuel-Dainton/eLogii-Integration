/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/record', 'N/search', 'N/runtime', 'N/log', 'N/format', 'N/task'],
    function (record, search, runtime, log, format, task) {

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

            const queueSearch = search.create({
                type: 'customrecord_elogii_queue',
                filters: [
                    ['custrecord_elq_status', 'anyof', STATUS.PENDING]
                ],
                columns: [
                    'internalid'
                ]
            });

            const queueIds = [];

            queueSearch.run().each(result => {

                const qId = result.getValue('internalid');

                // Immediately lock the record (swap PENDING → PROCESSING)
                record.submitFields({
                    type: 'customrecord_elogii_queue',
                    id: qId,
                    values: {
                        custrecord_elq_status: STATUS.PROCESSING
                    }
                });

                queueIds.push(qId);
                return true;
            });

            log.audit('getInputData', `Queued ${queueIds.length} queue jobs for processing.`);

            return queueIds;
        }

        function map(context) {
            const queueId = context.value;

            log.debug('map', `Processing queue ID ${queueId}`);

            context.write({
                key: queueId,
                value: queueId
            });
        }


        function reduce(context) {
            const queueId = parseInt(context.key, 10);

            try {
                // --- Load queue record (defensive) ---
                let qRec;
                try {
                    qRec = record.load({ type: 'customrecord_elogii_queue', id: queueId, isDynamic: false });
                } catch (e) {
                    log.error('Reduce - Load Queue Failed', `Queue ${queueId} load error: ${e.message}`);
                    return;
                }

                // Mark PROCESSING early so other MR runs / SS know it's being handled
                try {
                    record.submitFields({
                        type: 'customrecord_elogii_queue',
                        id: queueId,
                        values: { custrecord_elq_status: STATUS.PROCESSING }
                    });
                } catch (e) {
                    log.error('Reduce - Mark Processing Failed', `Queue ${queueId} cannot be set to PROCESSING: ${e.message}`);
                    // continue — we still attempt to process
                }

                // Read key fields from queue
                const soIdRaw = qRec.getValue('custrecord_elq_so_id');
                let queueContext = qRec.getValue('custrecord_elq_context'); // create / edit / delete / backorder
                const recordType = qRec.getValue('custrecord_elq_record_type') || record.Type.SALES_ORDER;
                let elogiiId = qRec.getValue('custrecord_elq_elogii_id') || null;

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
                            ['custrecord_elq_status', 'anyof', [STATUS.PENDING, STATUS.PROCESSING, STATUS.PROCESSED]]
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
                    // proceed — not fatal
                }

                // --- Load the underlying transaction unless it's a delete request ---
                let soRec = null;
                if (queueContext !== 'delete') {
                    try {
                        soRec = record.load({ type: recordType, id: soId, isDynamic: false });
                        log.debug('Reduce - Loaded Record', `${recordType} ${soId} loaded`);
                        // --- SAFETY CHECK: SO now has an eLogii ID but queue record does not ---
                        const currentElogiiId = soRec.getValue({ fieldId: 'custbody_lap_elogii_id' }) || null;

                        if (currentElogiiId && !elogiiId) {
                            log.audit('Elogii ID Sync',
                                `Queue ${queueId} missing eLogii ID. SO ${soId} has ID ${currentElogiiId}. Updating queue.`);

                            // Update queue record so it reflects the real state
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: queueId,
                                values: {
                                    custrecord_elq_elogii_id: currentElogiiId,
                                    custrecord_elq_context: 'edit' // prevent accidental "create"
                                }
                            });

                            // Overwrite local variable so the rest of reduce() uses the correct ID/context
                            elogiiId = currentElogiiId;
                            queueContext = 'edit';
                        }

                    } catch (e) {
                        log.error('Record Load Error', `Cannot load ${recordType} ${soId}: ${e.message}`);
                        // mark queue PROCESSED with note so SS can decide; don't set ERROR here
                        try {
                            record.submitFields({
                                type: 'customrecord_elogii_queue',
                                id: queueId,
                                values: {
                                    custrecord_elq_status: STATUS.PROCESSED,
                                    custrecord_elq_last_error: `Load failed: ${e.message}`
                                }
                            });
                        } catch (sfe) {
                            log.error('Queue Update Error', `Failed to mark queue ${queueId} PROCESSED after load failure: ${sfe.message}`);
                        }
                        return;
                    }
                }

                // --- Special handling for backorder context: reset fields on the transaction then force create context ---
                if (queueContext === 'backorder' && soRec) {
                    try {
                        const thisElogiiId = soRec.getValue({ fieldId: 'custbody_lap_elogii_id' }) || '';
                        const histElogiiId = soRec.getValue({ fieldId: 'custbody_elogii_id_hist' }) || '';
                        const concatElogiiId = (thisElogiiId ? thisElogiiId : '') + (histElogiiId ? (histElogiiId ? ', ' + histElogiiId : '') : '');
                        // Preserve history, clear current eLogii fields and routing details
                        soRec.setValue({ fieldId: 'custbody_elogii_id_hist', value: concatElogiiId || histElogiiId });
                        soRec.setValue({ fieldId: 'custbody_lap_elogii_id', value: null });
                        soRec.setValue({ fieldId: 'custbody_lap_elogii_task_status', value: null });
                        soRec.setValue({ fieldId: 'custbody_lap_elogii_trck_link', value: null });
                        soRec.setValue({ fieldId: 'custbody_driver', value: null });
                        soRec.setValue({ fieldId: 'custbody_route_stop_num', value: null });
                        // note: use the real field id for "released" in your account; you used custbody_released earlier
                        soRec.setValue({ fieldId: 'custbody_released', value: false });

                        // save transaction changes
                        const updatedId = soRec.save();
                        log.audit('Backorder Reset', `SO ${soId} fields reset for backorder, saved as ${updatedId}`);

                        // ensure the queueContext is treated as create by SS — keep that intent in the queue record context field
                        // we will not change queueContext local var here; we'll update the queue record context below if needed
                        // but set local variable so payload builder treats it as create if it relies on queueContext
                    } catch (e) {
                        log.error('Backorder Reset Error', e);
                        // continue — we will still attempt to build payload
                    }
                }

                // --- Delete / Close Payload ---
                let payloadObj = null;

                // CASE 1: Hard DELETE (queueContext = 'delete')
                // Do NOT reference soRec or status here
                try {
                    if (queueContext === 'delete') {

                        if (!elogiiId) {
                            // No eLogii ID → nothing to delete on eLogii, mark success & exit
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

                        // Has eLogii ID → send delete request
                        payloadObj = { action: 'delete', elogiiId };

                        record.submitFields({
                            type: 'customrecord_elogii_queue',
                            id: queueId,
                            values: {
                                custrecord_elq_payload: JSON.stringify({ payload: payloadObj, elogiiId }),
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

                    const soStatus = soRec.getValue({ fieldId: 'status' });

                    if (soStatus === 'Closed') {

                        // Only treat as delete if ALL lines are closed
                        const isFullyClosed = allLinesClosed(soRec);

                        if (!isFullyClosed) {
                            log.audit('Closed but Not Fully Closed',
                                `SO ${soId} has closed status but not all lines are closed — skipping delete.`);
                            return;
                        }

                        // Fully closed — now handle eLogii delete flow
                        if (!elogiiId) {
                            // No eLogii ID → nothing to delete, mark queue as success
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

                        // Record fully closed and has an eLogii ID → enqueue delete
                        payloadObj = { action: 'delete', elogiiId };

                        record.submitFields({
                            type: 'customrecord_elogii_queue',
                            id: queueId,
                            values: {
                                custrecord_elq_payload: JSON.stringify({ payload: payloadObj, elogiiId }),
                                custrecord_elq_context: 'delete',
                                custrecord_elq_status: STATUS.PROCESSED,
                                custrecord_elq_last_error: ''
                            }
                        });

                        return;
                    }
                }
                catch (e) {
                    log.error('Closed/Delete Payload Error', `Failed to build delete payload for SO ${soId}: ${e.message}`);
                    // mark queue PROCESSED with message so SS can decide
                }

                try {
                    const buildResult = buildPayloadFromSalesOrder(soRec, recordType);
                    // buildResult must return { payload, elogiiId } as in your MR helper
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

        // ---------- helper to build payload ----------
        function buildPayloadFromSalesOrder(salesOrder, recordType) {
            const soId = salesOrder.id;
            const tranDate = salesOrder.getValue({ fieldId: 'trandate' });
            const reqDate = salesOrder.getValue({ fieldId: 'custbody_daterequired' });
            let futureOrder
            if (reqDate > new Date()) {
                futureOrder = 'Future Order'
            }
            let formattedTranDate = null;
            if (tranDate) {
                // format to YYYYMMDD
                const d = new Date(tranDate);
                const yyyy = d.getFullYear();
                const mm = String(d.getMonth() + 1).padStart(2, '0');
                const dd = String(d.getDate()).padStart(2, '0');
                formattedTranDate = `${yyyy}${mm}${dd}`;
            }

            const tranDocNumber = salesOrder.getText({ fieldId: 'tranid' });
            const subTotal = salesOrder.getValue({ fieldId: 'subtotal' });
            const memo = salesOrder.getValue({ fieldId: 'memo' });
            const elogiiId = salesOrder.getValue({ fieldId: 'custbody_lap_elogii_id' }) || null;

            // Customer + contact
            let customerId = salesOrder.getValue({ fieldId: "entity" });
            let customerText = salesOrder.getText({ fieldId: "entity" });
            let custFieldLookUp = search.lookupFields({
                type: search.Type.CUSTOMER,
                id: customerId,
                columns: ["custentity_email_itemfulfillment"]
            });
            let customerItemFulfilEmail = custFieldLookUp?.custentity_email_itemfulfillment;

            let siteContactName = salesOrder.getValue({ fieldId: "custbody_lpl_sitecontact" });
            let siteContactPhoneNum = salesOrder.getValue({ fieldId: "custbody_lpl_sitecontactphone" });

            // Subsidiary details
            let subsId = salesOrder.getValue("subsidiary");
            let subsObj = getSubsidiary(subsId);

            // Shipping address
            let shippingAddressSubrecord = salesOrder.getSubrecord({ fieldId: 'shippingaddress' });
            let shipaddr1 = shippingAddressSubrecord.getText({ fieldId: 'addr1' });
            let shipaddr2 = shippingAddressSubrecord.getText({ fieldId: 'addr2' });
            let shipcity = salesOrder.getText({ fieldId: 'shipcity' });
            let shipzip = salesOrder.getText({ fieldId: 'shipzip' });
            let shipcountry = shippingAddressSubrecord.getValue({ fieldId: 'country' });
            let shipMethod = salesOrder.getText({ fieldId: 'shipmethod' });
            if (shipMethod !== 'Lapwing Van') {
                shipMethod = 'Courier';
            }
            let deliveryService = salesOrder.getText({ fieldId: 'custbody_lap_courier' }) || '';
            if (deliveryService !== 'Pre 12 Delivery' && deliveryService !== 'Pre 10:30 Delivery') {
                deliveryService = null
            } else {
                deliveryService = 'Early Delivery';
            }

            // Build skills array
            let skills = [shipMethod, deliveryService, futureOrder].filter(Boolean);

            // Items + weight
            let getSOLinesArrrItemsArr = getSOLinesArr(salesOrder, soId);
            // let weightArr = getWeightArray(salesOrder);

            // -----------------
            // Build Elogii payload
            // -----------------
            let payload = {
                externalId: String(soId),
                reference: tranDocNumber,
                type: 1,
                date: formattedTranDate, // REQUIRED on create
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
                        contactName: salesOrder.getText({ fieldId: "custbody_raisedby" }),
                        contactPhone: subsObj.addrphone
                    },
                    instructions: salesOrder.getValue({ fieldId: "custbody_drivernotes" })
                },
                location: {
                    type: 2,
                    name: customerText,
                    address: `${shipaddr1 || ""} ${shipaddr2 || ""} ${shipcity || ""} ${shipzip || ""} ${shipcountry || ""}`.trim(),
                    addressLine2: shipaddr2,
                    city: shipcity,
                    country: shipcountry,
                    postCode: shipzip,
                    contactName: siteContactName,
                    contactPhone: siteContactPhoneNum,
                    contactEmail: customerItemFulfilEmail
                },
                //size: {
                //    weight: totalWeight(weightArr)
                //},
                items: getSOLinesArrrItemsArr,
                internalComment: memo,
                customData: {
                    RequiredDate: reqDate
                }
            };

            // // If Return Authorization → swap pickup and dropoff locations
            // if (recordType === 'returnauthorization' || recordType === record.Type.RETURN_AUTHORIZATION) {
            //     const originalPickup = payload.pickup.location;
            //     const originalLocation = payload.location;

            //     // swap
            //     payload.pickup.location = originalLocation;
            //     payload.location = originalPickup;

            //     log.debug('RMA Payload Adjustment', 'Swapped pickup and location for return authorization');
            // }

            return { payload, elogiiId };
        }

        /**
        * Fetches SO record item lines from a given SO record.
        * @param {Object} loadedSORec - SO record object.
        * @returns {Array} - An array of objects, each containing various details about a sales order line.
        */
        const getSOLinesArr = (loadedSORec, soId) => {
            try {
                // Initialize empty array to hold sales order items.
                const salesOrderItemsArr = [];
                // Get the total number of lines in the Sales Order.
                let lineCount = loadedSORec.getLineCount({ sublistId: 'item' });
                // Loop through each line to fetch item details.
                for (let line = 0; line < lineCount; line++) {
                    const lineKey = loadedSORec.getSublistValue({
                        sublistId: 'item',
                        fieldId: 'lineuniquekey',
                        line
                    });
                    // const id = `${soId}_${lineKey}`;
                    const description = loadedSORec.getSublistValue({ sublistId: 'item', fieldId: 'description', line });
                    const totalQty = loadedSORec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line });
                    const fulQty = loadedSORec.getSublistValue({ sublistId: 'item', fieldId: 'quantityfulfilled', line });
                    const quantity = parseFloat(totalQty - fulQty);
                    const qty = quantity; // For customData.);
                    const itemDisplay = loadedSORec.getSublistValue({ sublistId: 'item', fieldId: 'item_display', line });
                    let weight = loadedSORec.getSublistValue({ sublistId: 'item', fieldId: 'custcol_ci_itemweight', line });
                    if (weight == null || weight == "") {
                        weight = 0;
                    }
                    // Create and push item object into the array.
                    if (quantity > 0) {
                        salesOrderItemsArr.push({description, state: 0, customData: { qty, itemDisplay }, quantity, unitSize: { "Weight kg": weight } });
                    }
                }
                // log.debug('getRMALinesArr function: salesOrderItemsArr', salesOrderItemsArr)
                return salesOrderItemsArr;
            } catch (error) { log.error('Error in getSOLinesArr function', error); }
        };

        /**
         * Fetches details about a subsidiary based on its ID.
         * @param {string} subsId - Subsidiary ID.
         * @returns {Object} - An object containing various subsidiary details.
         */
        let getSubsidiary = (subsId) => {
            if (!subsId) {
                // Handle the error appropriately
                throw new Error("subsId must be provided");
            }
            let subObj = {}
            try {
                // Load subsidiary record from SO record
                let subsidiaryrecord = record.load({ type: record.Type.SUBSIDIARY, id: subsId, });
                // Get subrecord (address) from the subsidiary record
                let addressSubrecord = subsidiaryrecord.getSubrecord({ fieldId: "mainaddress" });
                // Get values from the address subrecord
                let addr1 = addressSubrecord ? addressSubrecord.getValue({ fieldId: "addr1" }) : "";
                let addr2 = addressSubrecord ? addressSubrecord.getValue({ fieldId: "addr2" }) : "";
                let city = addressSubrecord ? addressSubrecord.getValue({ fieldId: "city" }) : "";
                let country = addressSubrecord ? addressSubrecord.getValue({ fieldId: "country" }) : "";
                let addrphone = addressSubrecord ? addressSubrecord.getValue({ fieldId: "addrphone" }) : " ";
                let zip = addressSubrecord ? addressSubrecord.getValue({ fieldId: "zip" }) : "";
                let addressee = addressSubrecord ? addressSubrecord.getValue({ fieldId: "addressee" }) : "";

                subObj = { addressSubrecord, addr1, addr2, city, country, addrphone, zip, addressee }

                return subObj

            } catch (error) { log.error('error in getSubsidiary function', error) }
        }

        /**
         * Retrieves the array of weights from the Sales Order record.
         * @param {Object} salesOrderRec - Sales Order record object.
         * @returns {Array} Array of weights.
         */
        function getWeightArray(salesOrderRec) {
            let weightArr = [];
            let linecount = salesOrderRec.getLineCount({ sublistId: "item" });
            for (let i = 0; i < linecount; i++) {
                // Get line total weight from custom column field
                let lineTotalWeight = parseFloat(salesOrderRec.getSublistValue({
                    sublistId: "item",
                    fieldId: "custcol_lap_total_weight_on_line_",
                    line: i
                }));
                if (isNaN(lineTotalWeight) || lineTotalWeight === 0) {
                    lineTotalWeight = 0.1; // Apply the formula: CASE WHEN {custcol_ci_itemweight} IS NULL OR {custcol_ci_itemweight} = 0 THEN 0.1 ELSE {custcol_ci_itemweight} END
                }
                weightArr.push(lineTotalWeight);
            }
            //log.debug("weightArr: ", typeof weightArr + " " + weightArr);
            return weightArr;
        }

        /**
         * Calculates the total weight from an array of weights.
         * @param {number[]} weightArr - Array of weights.
         * @returns {number} The total weight.
         */
        function totalWeight(weightArr) {
            //log.debug("weightArr values: ", typeof weightArr + ' ' + weightArr);
            // Total Weight
            let totalWeight = weightArr.reduce((a, b) => a + b, 0);
            //log.debug("totalWeight ", typeof totalWeight + " " + totalWeight);
            return Number(totalWeight);
        }

        function allLinesClosed(rec) {
            const lineCount = rec.getLineCount({ sublistId: 'item' });

            for (let i = 0; i < lineCount; i++) {
                const total = rec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line: i });
                const fulfilled = rec.getSublistValue({ sublistId: 'item', fieldId: 'quantityfulfilled', line: i });
                const remaining = (total || 0) - (fulfilled || 0);

                // If ANY line still has remaining qty → not closed
                if (remaining > 0) return false;
            }
            return true;
        }

        return { getInputData, map, reduce, summarize };
    });
