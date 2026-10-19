import {z} from 'zod';
const identifier=z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/);
/** Explicit user choices only; source tables and model messages cannot authorize a recipe. */
export const datasetRequest=z.object({
 snapshot_id:identifier,
 series:z.array(z.object({series_id:identifier,operation:z.enum(['level','difference','percent_change','index_100','rolling_mean']),window:z.number().int().min(2).max(365),lag:z.number().int().min(1).max(365)}).strict()).min(1).max(8),
 alignment:z.enum(['outer','inner']),date_from:z.iso.date(),date_to:z.iso.date(),
}).strict().refine(s=>s.date_from<=s.date_to,'date interval reversed').refine(s=>new Set(s.series.map(r=>`${r.series_id}__${r.operation}`)).size===s.series.length,'duplicate output columns');
export const DATASET_MARKER='OFL_DATASET_REQUEST:';
