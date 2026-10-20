---
name: rust-review
description: Review Rust source for correctness, async safety, error handling, and maintainability.
---

# Rust Code Review Workflow

1. Confirm objective and file scope from the user/delegation prompt.
2. Read affected source and relevant tests with allowed tools.
3. Check Rust ownership/borrows, concurrency (`Send`, `Sync`, cancellation), error handling, and SQL transaction boundaries.
4. Report findings with source file references and confidence; clearly mark uncertain or unverified claims.
5. Do not modify source unless explicitly authorized by available tools and task instructions.

The response is free-form final text; this skill does not impose mandatory JSON output.
