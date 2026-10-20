"""Passage windows over whitespace words, bounded by word count and estimated tokens.

A window holds at most WORDS words and at most TOKEN_BUDGET estimated tokens; the next
window repeats the last OVERLAP/WORDS of it. Text whose windows all fit the budget is
cut exactly as the original word-only windows, so its passages keep their identities.
"""

WORDS = 350
OVERLAP = 40
# The deployed embedding model refuses inputs over 2048 tokens. On the production
# corpus (BGE-M3 tokenizer) the largest embedded passage had 1,913 tokens and the
# smallest refused one 2,409. Two of the 2048 positions hold the sequence markers.
CONTEXT_TOKENS = 2048
TOKEN_BUDGET = CONTEXT_TOKENS - 2
# Part of a passage revision only when the token budget changes a document's windows.
VERSION = "token-bound-v1"


def word_cost(word):
    """Estimated tokens of one word, in quarter tokens.

    One token per word, plus half a token per ASCII letter or punctuation mark, three
    quarters per ASCII digit and one per other character. On the production corpus the
    estimate stayed above the BGE-M3 count for every passage over 256 tokens (actual
    tokens at most 0.87 of it), including words glued together by PDF text extraction,
    numeric tables and non-Latin scripts. Random base64-like strings without spaces can
    still exceed it; the embedding step rejects those instead of truncating them.
    """
    return 4 + sum((3 if ch.isdigit() else 2) if ch.isascii() else 4 for ch in word)


def overlap(size):
    """Words a window of `size` words shares with the next one (40 for a full window)."""
    return size * OVERLAP // WORDS


def windows(words, budget=TOKEN_BUDGET):
    """(start, end) word spans; `budget=None` reproduces the word-only windows.

    A single word over the budget still forms its own window: it is never cut inside.
    """
    limit = None if budget is None else budget * 4
    spans, start = [], 0
    while start < len(words):
        end, cost = start, 0
        while end < min(start + WORDS, len(words)):
            cost += word_cost(words[end])
            if limit is not None and end > start and cost > limit:
                break
            end += 1
        spans.append((start, end))
        if end == len(words):
            break
        start = end - overlap(end - start)
    return spans
