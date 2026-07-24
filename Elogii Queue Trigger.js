/**
 * @NApiVersion 2.1
 * @NScriptType UserEventScript
 */
define(['N/record', 'N/runtime', 'N/task', 'N/ui/serverWidget', 'N/url', 'N/search'],
    function (record, runtime, task, serverWidget, url, search) {

        // Customer collection should change ship method
        // Set up Weight kg and calculate items x quantity in the Elogii Settings > Dimensions

        const STATUS = {
            PENDING: 1,
            RETRY: 2,
            SUCCESS: 3,
            ERROR: 4,
            PROCESSING: 5,
            PROCESSED: 6
        };

        function beforeLoad(context) {
            try {
                //----------------------------------------------------------
                // CLEAN INHERITED SALES ORDER FIELDS ON RMA CREATION
                //----------------------------------------------------------
                if ((context.newRecord.type === 'returnauthorization' &&
                    context.type === context.UserEventType.CREATE) || (context.type === context.UserEventType.COPY)) {

                    const rec = context.newRecord;

                    const inheritedElogiiId = rec.getValue({ fieldId: 'custbody_lap_elogii_id' });
                    const existingHist = rec.getValue({ fieldId: 'custbody_elogii_id_hist' }) || '';

                    if (inheritedElogiiId) {
                        const newHist = existingHist
                            ? `${existingHist}, ${inheritedElogiiId}`
                            : inheritedElogiiId;

                        rec.setValue({ fieldId: 'custbody_elogii_id_hist', value: newHist });
                    }

                    rec.setValue({ fieldId: 'custbody_lap_elogii_id', value: '' });
                    rec.setValue({ fieldId: 'custbody_lap_elogii_trck_link', value: '' });
                    rec.setValue({ fieldId: 'custbody_lap_elogii_task_status', value: '' });
                    rec.setValue({ fieldId: 'custbody_driver', value: '' });
                    rec.setValue({ fieldId: 'custbody_route_stop_num', value: '' });
                    rec.setValue({ fieldId: 'custbody_released', value: false });
                    rec.setValue({ fieldId: 'custbody_elogii_internal_id', value: '' });

                    log.audit('RMA Cleanup (beforeLoad)', `Reset inherited eLogii fields for new RMA ${rec.id}`);
                }

                //----------------------------------------------------------
                // ADD BACKORDER BUTTON
                //----------------------------------------------------------

                if (context.type !== context.UserEventType.VIEW) return;

                const form = context.form;
                const rec = context.newRecord;
                const soId = rec.id;

                const releaseToElogii = rec.getValue({ fieldId: 'custbody_release_to_elogii' });
                const elogiiId = rec.getValue({ fieldId: 'custbody_lap_elogii_id' });

                if (rec.type !== 'returnauthorization' && ((releaseToElogii === true && elogiiId) || (releaseToElogii === 'T' && elogiiId))) {

                    // Only show button if today is NOT the created date
                    const createdDate = rec.getValue({ fieldId: 'trandate' });
                    const today = new Date();
                    const created = new Date(createdDate);

                    // Strip time component from both dates for a pure date comparison
                    today.setHours(0, 0, 0, 0);
                    created.setHours(0, 0, 0, 0);

                    const isCreatedToday = today.getTime() === created.getTime();

                    if (!isCreatedToday) {
                        const suiteletUrl = url.resolveScript({
                            scriptId: 'customscript_elogii_backorder_sl',
                            deploymentId: 'customdeploy_elogii_backorder_sl',
                            params: { soId }
                        });

                        // Wrap the suitelet call in a confirm() popup
                        const confirmMsg = 'You are about to create a new task in eLogii for the purpose of completing a partially fulfilled or forgotten order. Press OK to continue.';

                        // Inject the named function into the page as inline HTML
                        const scriptField = form.addField({
                            id: 'custpage_elogii_backorder_script',
                            type: serverWidget.FieldType.INLINEHTML,
                            label: 'Script'
                        });

                        scriptField.defaultValue = `
                           <script>
                            function openElogiiBackorder() {
                           if (confirm(${JSON.stringify(confirmMsg)})) {
                            window.open('${suiteletUrl}', '_blank');
                            }
                          }
                          </script>
                          `;

                        form.addButton({
                            id: 'custpage_elogii_backorder',
                            label: 'Send Backorder to eLogii',
                            functionName: 'openElogiiBackorder()'
                        });
                    }
                }

                if (rec.type === 'returnauthorization') {
                    try { form.removeButton('closeremaining'); } catch (e) { }
                    log.debug('RMA UI', 'Close button removed');
                }

            } catch (e) {
                log.error('beforeLoad error', e);
            }
        }

        // ---------------------------
        // AFTER SUBMIT
        // ---------------------------
        function afterSubmit(context) {
            try {
                const salesOrderId = context.newRecord.id;
                let eventType = context.type;

                const elogiiId = context.newRecord.getValue({ fieldId: 'custbody_lap_elogii_id' });
                const newRelease = context.newRecord.getValue({ fieldId: 'custbody_release_to_elogii' });
                const oldRelease = context.oldRecord ? context.oldRecord.getValue({ fieldId: 'custbody_release_to_elogii' }) : null;
                const newPickup = context.newRecord.getValue({ fieldId: 'custbody_lap_cust_pickup' });
                const elogiiInternalId = context.newRecord.getValue({ fieldId: 'custbody_elogii_internal_id' });

                log.debug('afterSubmit Triggered', 'context.type=' + context.type + ', SO/RMA ID=' + salesOrderId);

                //----------------------------------------------------------
                // 1. RELEASE CHECK FIRST -> global bypass switch
                //----------------------------------------------------------
                if (newRelease !== true) {
                    log.debug('UE Skipped', `Release to eLogii not enabled for record ${salesOrderId}`);
                    return;
                }

                //----------------------------------------------------------
                // 2. If Release was toggled ON on this edit -> treat as CREATE
                //----------------------------------------------------------
                if (oldRelease !== newRelease) {
                    eventType = 'create';
                    log.debug('UE Override', `Release toggled ON for record ${salesOrderId}, context set to CREATE`);
                }

                //----------------------------------------------------------
                // 4. On EDIT: cleanup if values CHANGED into disqualified state
                //----------------------------------------------------------
                if (context.type === context.UserEventType.EDIT && context.oldRecord) {

                    const oldPickup = context.oldRecord.getValue({ fieldId: 'custbody_lap_cust_pickup' });

                    const pickupChangedOn =
                        (!oldPickup && newPickup === true);

                    if (pickupChangedOn) {

                        log.audit('Cleanup Triggered', `pickupChangedOn=${pickupChangedOn}`);

                        //--------------------------------------------------
                        // A. If eLogii ID exists -> enqueue DELETE
                        //--------------------------------------------------
                        if (elogiiId) {
                            const delRec = record.create({
                                type: 'customrecord_elogii_queue',
                                isDynamic: true
                            });

                            delRec.setValue('custrecord_elq_so_id', salesOrderId);
                            delRec.setValue('custrecord_elq_payload', JSON.stringify({
                                action: 'delete',
                                elogiiId: elogiiId
                            }));
                            delRec.setValue('custrecord_elq_status', STATUS.PENDING);
                            delRec.setText('custrecord_elq_context', 'delete');
                            delRec.setValue('custrecord_elq_elogii_internal_id', elogiiInternalId || '');

                            const qId = delRec.save();
                            log.audit('Delete queued', `Queue ID ${qId}`);
                        }
                        //--------------------------------------------------
                        // B. No eLogii ID -> clean existing queue records
                        //--------------------------------------------------
                        else {
                            const qSearch = search.create({
                                type: 'customrecord_elogii_queue',
                                filters: [['custrecord_elq_so_id', 'is', salesOrderId]],
                                columns: ['internalid']
                            });

                            qSearch.run().each(r => {
                                record.submitFields({
                                    type: 'customrecord_elogii_queue',
                                    id: r.getValue('internalid'),
                                    values: {
                                        custrecord_elq_status: STATUS.SUCCESS,
                                        custrecord_elq_last_error: 'Removed due to customer pickup turned on.'
                                    }
                                });
                                return true;
                            });
                        }

                        // Stop normal processing
                        return;
                    }
                }

                if (![context.UserEventType.CREATE, context.UserEventType.EDIT, context.UserEventType.COPY, context.UserEventType.DELETE].includes(eventType)) {
                    log.debug('UE Triggered', 'Not a relevant event, exiting. SO ID: ' + salesOrderId);
                    return;
                }

                let shouldTrigger = false;

                if (eventType === context.UserEventType.EDIT && context.oldRecord) {
                    const newRec = context.newRecord;
                    const oldRec = context.oldRecord;

                    // Body fields to compare
                    const fieldsToCheck = [
                        'custbody_daterequired',
                        'custbody_lpl_sitecontact',
                        'custbody_lpl_sitecontactphone',
                        'shipmethod',
                        'memo',
                        'custbody_release_to_elogii'
                    ];

                    // Address subfields to compare individually
                    const addressFields = [
                        'shipcountry',
                        'shipaddressee',
                        'shipaddr1',
                        'shipaddr2',
                        'shipcity',
                        'shipstate',
                        'shipzip'
                    ];

                    for (let field of [...fieldsToCheck, ...addressFields]) {
                        const newVal = newRec.getValue({ fieldId: field });
                        const oldVal = oldRec.getValue({ fieldId: field });

                        const normalizedNew = newVal ? newVal.toString().trim() : '';
                        const normalizedOld = oldVal ? oldVal.toString().trim() : '';

                        if (normalizedNew !== normalizedOld) {
                            shouldTrigger = true;
                            log.audit(`Field Changed: ${field}`, `Old: ${normalizedOld} | New: ${normalizedNew}`);
                            break;
                        }
                    }

                    // Compare item lines only if no field-level changes found
                    if (!shouldTrigger) {
                        const newLineCount = newRec.getLineCount({ sublistId: 'item' });
                        const oldLineCount = oldRec.getLineCount({ sublistId: 'item' });

                        if (newLineCount !== oldLineCount) {
                            shouldTrigger = true;
                            log.audit('Line Count Changed', `Old: ${oldLineCount} | New: ${newLineCount}`);
                        } else {
                            for (let i = 0; i < newLineCount; i++) {
                                const newItem = newRec.getSublistValue({ sublistId: 'item', fieldId: 'item', line: i });
                                const newQty = newRec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line: i });
                                const oldItem = oldRec.getSublistValue({ sublistId: 'item', fieldId: 'item', line: i });
                                const oldQty = oldRec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line: i });

                                if (newItem !== oldItem || newQty !== oldQty) {
                                    shouldTrigger = true;
                                    log.audit('Item/Qty Change Detected', `Line ${i} | Old Item: ${oldItem}, New Item: ${newItem}, Old Qty: ${oldQty}, New Qty: ${newQty}`);
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    shouldTrigger = true; // always trigger for CREATE, COPY, DELETE
                }

                if (!shouldTrigger) {
                    log.debug('No significant changes detected', 'Skipping MR task.');
                    return;
                }

                if (!elogiiId && eventType !== context.UserEventType.DELETE) {
                    eventType = 'create';
                    log.debug(
                        'UE Override',
                        `No eLogii ID for record ${salesOrderId}, context set to CREATE`
                    );
                }

                const recordType = context.newRecord.type;
                const cleanId = String(parseInt(salesOrderId, 10));

                const qRec = record.create({
                    type: 'customrecord_elogii_queue',
                    isDynamic: true
                });

                qRec.setValue({
                    fieldId: 'custrecord_elq_so_id',
                    value: cleanId
                });

                qRec.setValue({
                    fieldId: 'custrecord_elq_context',
                    value: eventType
                });

                qRec.setValue({
                    fieldId: 'custrecord_elq_record_type',
                    value: recordType
                });

                qRec.setValue({
                    fieldId: 'custrecord_elq_elogii_id',
                    value: elogiiId || ''
                });

                qRec.setValue({
                    fieldId: 'custrecord_elq_status',
                    value: STATUS.PENDING
                });

                qRec.setValue({
                    fieldId: 'custrecord_elq_next_run',
                    value: new Date()
                });

                qRec.setValue({
                    fieldId: 'custrecord_elq_elogii_internal_id',
                    value: elogiiInternalId || ''
                });


                const qId = qRec.save();
                log.audit('Queue Created', `Queue ID ${qId} for ${recordType} ${cleanId} (${eventType})`);

            }
            catch (e) {
                log.error('Error in afterSubmit UE', e);
            }
        }

        return {
            afterSubmit: afterSubmit,
            beforeLoad: beforeLoad
        };
    });
