# DESIGN.md — OFL Ledger

Register: **product**. Color strategy: **Restrained** (tinted neutrals + one accent, kept
under ~10% of surface).

## Theme: light

Decided from the scene in PRODUCT.md, not from category. This is read next to BACEN and
IBGE tabs by someone about to cite a figure, and a source that gets cited looks like a
document. The previous dark-terminal treatment was the reflex one tier down from
navy-and-gold, and it made a statistical almanac look like a console.

## Color (OKLCH)

Neutrals are tinted warm (hue 75) so the page reads as paper rather than as a spreadsheet.
No `#fff`, no `#000`.

| Token | Value | Use |
|---|---|---|
| `--paper` | `oklch(0.985 0.004 75)` | Page |
| `--paper-sunk` | `oklch(0.965 0.006 75)` | Rails, table zebra, inset blocks |
| `--rule` | `oklch(0.90 0.008 75)` | Hairlines. The main structural device |
| `--rule-strong` | `oklch(0.80 0.010 75)` | Section rules |
| `--ink` | `oklch(0.24 0.012 75)` | Body, figures |
| `--ink-muted` | `oklch(0.52 0.010 75)` | Labels, units, metadata |
| `--accent` | `oklch(0.48 0.13 245)` | Links, current nav, focus ring |

### Status vocabulary, and the rule that governs it

Four states, and only the first two are ever coloured:

| State | Token | Meaning |
|---|---|---|
| `ok` | `oklch(0.55 0.11 155)` | Published, inside its freshness budget |
| `late` | `oklch(0.55 0.16 28)` | **Published and past its budget.** The only alarm |
| `withheld` | `--ink-muted` on `--paper-sunk` | Absent by written licence verdict |
| `absent` | `--ink-muted`, no dot | Registered, not in this release |

`withheld` and `absent` get **no colour and no dot**. They are facts, not failures. A page
where 86% of rows glow red has trained its reader to ignore red, which costs exactly the
signal the page exists to carry.

## Typography

One family for the interface (Hanken Grotesk), one for figures (JetBrains Mono, tabular).
Fraunces is retained **only** for the page title, one instance per page: the almanac
signature. It never appears in labels, buttons or data.

Fixed rem scale, ratio ~1.2: `0.75 / 0.8125 / 0.875 / 1 / 1.25 / 1.5 / 2rem`.

## Layout

- Content column `max-width: 68rem`, gutters `1.5rem` → `3rem`. The zigzag this replaces
  came from full-width rows on a 2000px viewport with no container at all.
- **Rules, not cards.** Sections are separated by hairlines. No card grids, no nesting.
- Status lists are **tables** with fixed columns, so the eye tracks down a column instead
  of hunting across a stretched row.
- Left rail for navigation at `md+`, horizontal scroller below it.

## Motion

Transitions 160ms `ease-out-quart`, on colour and opacity only. No layout animation, no
page-load choreography. This is a reference; it loads and it is there.
