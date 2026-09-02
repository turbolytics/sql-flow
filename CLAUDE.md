# Writing style

Write prose in the style of [Google Technical Writing One](https://developers.google.com/tech-writing/one). This applies to commit messages, pull request descriptions, code comments, documentation, and replies in the terminal.

## Words

Use one term for one thing. Do not alternate between synonyms for the same concept.

Spell out an acronym on first use, then use the acronym. Skip the acronym entirely when a term appears fewer than about four times.

Define a term the reader may not know, or link to a definition.

## Sentences

Follow these rules:

- Focus each sentence on a single idea.
- Use active voice. Reserve passive voice for the rare sentence whose actor genuinely does not matter.
- Pick a specific verb over a vague one. `The pipeline commits the offset` beats `The offset is handled by the pipeline`.
- Split a subordinate clause into its own sentence when it carries a separate idea.
- Use `that` for an essential clause and take no comma. Use `which` for a nonessential clause and take a comma.

Cut filler words. Replace these phrases:

| Wordy | Concise |
|---|---|
| at this point in time | now |
| in order to | to |
| is able to | can |
| determine the location of | find |
| provides a detailed description of | describes |
| due to the fact that | because |

## Lists and tables

Introduce every list and table with a sentence that ends in a colon.

Use a bulleted list for unordered items. Use a numbered list for ordered items, and start each item with an imperative verb.

Keep list items parallel. The first item sets a pattern of grammar, category, capitalization, and punctuation that every later item must match.

Capitalize the first word of each list item. Punctuate an item that is a full sentence as a sentence.

## Paragraphs

State the paragraph's point in its first sentence.

Give each paragraph one topic. Move a second topic into its own paragraph.

State the key point at the start of a document, not at the end.

## Comments and commit messages

Explain why the code does something. The code already shows what it does.

Write a commit message that names the defect, the fix, and the evidence. State what a reader loses if the change is wrong.

## What this style rejects

Avoid these habits:

- Hedging that carries no information, such as `it seems that` or `arguably`.
- Marketing adjectives, such as `powerful`, `seamless`, or `robust`.
- A closing sentence that repeats the opening sentence.
- Em dashes and parenthetical asides where a second sentence is clearer.
