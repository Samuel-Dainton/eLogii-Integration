/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 */
define(['N/record', 'N/ui/serverWidget', 'N/log'], function (record, ui, log) {

    const STATUS = {
        PENDING: 1,
        RETRY: 2,
        SUCCESS: 3,
        ERROR: 4,
        PROCESSING: 5,
        PROCESSED: 6
    };

    function onRequest(context) {
        const soId = context.request.parameters.soId;

        log.audit('Backorder Triggered', `Received SO ID: ${soId}`);

        try {
            // Load record only to identify type + elogii ID
            // You MUST know if it's a Sales Order or RMA
            let recordType = null;
            try {
                record.load({ type: record.Type.SALES_ORDER, id: soId });
                recordType = record.Type.SALES_ORDER;
            } catch (e1) {
                try {
                    record.load({ type: record.Type.RETURN_AUTHORIZATION, id: soId });
                    recordType = record.Type.RETURN_AUTHORIZATION;
                } catch (e2) {
                    throw new Error(`Record ${soId} is neither SO nor RMA.`);
                }
            }

            const loadedRec = record.load({ type: recordType, id: soId });

            const elogiiId = loadedRec.getValue({ fieldId: 'custbody_lap_elogii_id' }) || '';

            // Create new queue row
            const qRec = record.create({
                type: 'customrecord_elogii_queue',
                isDynamic: true
            });

            qRec.setValue({
                fieldId: 'custrecord_elq_so_id',
                value: soId
            });

            qRec.setValue({
                fieldId: 'custrecord_elq_context',
                value: 'backorder'
            });

            qRec.setValue({
                fieldId: 'custrecord_elq_record_type',
                value: recordType
            });

            qRec.setValue({
                fieldId: 'custrecord_elq_elogii_id',
                value: elogiiId
            });

            qRec.setValue({
                fieldId: 'custrecord_elq_status',
                value: STATUS.PENDING
            });

            qRec.setValue({
                fieldId: 'custrecord_elq_next_run',
                value: new Date()
            });

            const queueId = qRec.save();

            // Show response
            const form = ui.createForm({ title: 'eLogii Backorder' });
            form.addField({
                id: 'custpage_result',
                type: ui.FieldType.INLINEHTML,
                label: ' '
            }).defaultValue = `
                <h2>Backorder queued successfully!</h2>
                <p>Queue Record ID: ${queueId}</p>
                <p>You can close this window.</p>
            `;

            context.response.writePage(form);

        } catch (e) {
            log.error('Error creating backorder queue record', e);

            const form = ui.createForm({ title: 'Error' });
            form.addField({
                id: 'custpage_error',
                type: ui.FieldType.INLINEHTML,
                label: ' '
            }).defaultValue = `<p style="color:red;">Error: ${e.message}</p>`;
            context.response.writePage(form);
        }
    }

    return { onRequest };
});
