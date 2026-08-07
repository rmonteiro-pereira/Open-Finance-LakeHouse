# PRODUCT.md — OFL Ledger

**register:** product

## What this is

The reader surface of the Open-Finance LakeHouse: Brazilian macro and fixed-income
numbers, published from a versioned release artefact. Not a BI tool, not a trading
terminal. A **reference you cite**.

## Users

1. **Someone about to quote a number.** An analyst pasting the ex-ante real rate into a
   note, a journalist checking whether IPCA 12m is what they think it is. They need the
   figure, its unit, its as-of date, and a link they can send to whoever asks.
2. **Someone deciding whether to depend on this.** An engineer evaluating the data source
   before wiring it into their own product. They go straight to `/confianca` and ask what
   breaks and how they would know.
3. **Someone assessing the author.** A hiring engineer. They are not the target, but every
   choice here is visible to them, so nothing may be decorative.

## The scene

Mid-morning, laptop, alongside browser tabs of BACEN, IBGE and Tesouro Transparente. The
question in the user's head is always some form of *"can I say this out loud?"*

That scene forces **light**. A source that gets cited looks like a document, not like a
console. Dark would make this the odd tab in a row of statistical agencies, and would
signal "developer tool" when the product is "a number you can defend".

## Tone

Precise, unhedged, quantitative. Portuguese. The interface states what it knows and names
what it does not. Never reassuring, never alarming: those are both editorialising.

## Strategic principles

1. **A number never travels without its unit and its as-of date.** `percent` alone covered
   the daily SELIC, monthly IPCA variation and debt/GDP. The unit is a tuple, and it shows.
2. **A colour that is always on says nothing.** Red means "published and late". A series
   that is absent by licence verdict is not late, and painting it red is not honesty, it is
   noise that destroys the signal red is for.
3. **Absence has a reason, and the reason is data.** Withheld, unverified, not ingested and
   stale are four different states. Collapsing them into "no data" is a bug with a
   stylesheet.
4. **Nothing is written by hand on a status page.** If a figure could be improved by editing
   the page, the page can lie.

## Anti-references

- **Trading terminals** (Bloomberg, TradingView). Wrong job. Nobody here is taking a
  position; they are deciding whether to trust a figure.
- **The SaaS metric hero.** Big gradient number, small label, three supporting stats.
- **Dark "data platform" landing pages.** The second-order reflex for anything fintech that
  refuses navy-and-gold. This project is a statistical almanac, not a console.
- **Status pages that are all green by construction.** A dashboard whose lights cannot go
  red is decoration. So is one where they are all red.
