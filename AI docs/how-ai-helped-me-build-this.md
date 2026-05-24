# How AI Helped Me Build This

This project wasn't built to learn Kafka. **It was built to learn AI** — how it thinks, how it responds, and how to actually get production-quality work done through conversation alone. Kafka and the multi-language roadmap are the vehicle.

---

## The Real Goal

Most AI usage I see is autocomplete — tab to accept a suggestion. I wanted to go further: can I have a genuine engineering conversation with an AI agent and come out the other end with something that works, is well-structured, and follows real standards?

The answer is yes. But the skill isn't in the prompting — it's in **knowing when to trust it, when to override it, and how to read what it gives you**.

---

## What I Learned About AI

### How it interprets prompts
Some prompts in this project were one sentence. Some were detailed. The difference in output quality taught me where precision actually matters:
- **Vague prompts work for scaffolding** — "build an event-driven app with Kafka, Postgres, MongoDB" produced a complete, working structure
- **Precision matters for behaviour** — "GET should happen from Mongo" was enough; AI understood the architectural intent without needing implementation detail
- **Short commands work in context** — "remove the tests" was all it took because AI had read the whole codebase

### When AI decides vs asks
AI never asked for clarification when it could take action. It acted first, then explained. This is useful once you understand the pattern — if you want AI to ask, you have to explicitly say so.

### How AI handles failure
When the Testcontainers setup failed, AI tried three different approaches before I cut scope. Watching it work through the problem — changing the MongoDB URI strategy each time — showed me how it reasons iteratively rather than in one shot.

### When to override it
Every architectural decision was mine:
- CQRS reads from MongoDB — my call
- The multi-language roadmap — my idea
- Renaming the repo — my decision

AI executed all of them without question. **Knowing when to take the wheel is the actual skill.**

### How to audit AI output
Asking "does this follow best practices?" found 6 real bugs that would have caused production failures — wrong Kafka port, missing validation, lazy loading crash, wrong dependency names. That single prompt is now a permanent part of my workflow.

---

## What AI Generated

| Component | What was generated |
|---|---|
| Project scaffold | `build.gradle`, package structure, Docker Compose, `application.yml` |
| Domain model | `Employee`, `Department`, `EmployeeEventDocument` with JPA, Lombok, MongoDB annotations |
| Avro schema | `message.avsc` with correct logical types — decimal, date, nested record, enum |
| Kafka producer | Async send with headers, CompletableFuture callbacks, structured logging |
| Kafka consumer | Header extraction, MongoDB persistence, null payload guard |
| MapStruct mapper | `@Context` for event type, expression mappings, nested builder |
| REST layer | Controllers, services, repositories, DTOs, validation, error handling |
| Integration tests | Real Testcontainers setup — Postgres, Kafka, Schema Registry, MongoDB |
| All documentation | README, AI docs, conversation log — written from actual code state |

---

## What I Decided

- **Architecture** — CQRS split: writes to Postgres, reads from MongoDB
- **Scope** — dropped tests when they blocked progress, came back to them later
- **Standards** — asked for an audit; reviewed and accepted each fix
- **Roadmap** — same app, multiple languages, to compare AI behaviour across ecosystems
- **Documentation style** — LinkedIn-pitch framing, learning narrative, not just technical docs

---

## The Multi-Language Experiment

Now that Java is done, I'll rebuild the **exact same application** in Go, Python, and Node.js — using the same prompt, the same AI tool, and watching what changes:

- Does AI need more or less guidance in a dynamically typed language?
- How does it handle a language with no established "framework" for everything?
- Where does it make different tradeoffs?
- Which language produces the least boilerplate with AI assistance?

The prompt to do this is in [`prompt-new-language.md`](./prompt-new-language.md).
