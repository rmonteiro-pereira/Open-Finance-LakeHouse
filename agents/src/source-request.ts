import {z} from 'zod';
/** IDs are requested reads, never trusted source text, authority or review verdicts. */
export const SOURCE_MARKER='OFL_SOURCE_REQUEST:';
export const sourceRequest=z.object({
 version:z.literal(1),question:z.string().trim().min(1).max(1600),
 passage_ids:z.array(z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/)).min(1).max(4),
}).strict().refine(s=>new Set(s.passage_ids).size===s.passage_ids.length,'duplicate passage IDs');
