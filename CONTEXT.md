# Observability Intelligence

This context turns tenant-owned runtime facts into durable, evidence-backed investigations and billable operational outcomes.

## Language

**Observation**:
An immutable runtime fact from an application, model, tool, workflow, or Agent execution.
_Avoid_: Event, log item

**Evidence**:
An Observation that has been explicitly selected and tenant-validated for an Investigation.
_Avoid_: Context, raw data

**Investigation**:
A durable, recoverable attempt to explain a concrete operational objective from a fixed Evidence set.
_Avoid_: Agent run, chat, plan

**Investigation Step**:
A server-selected, policy-bound unit of work within an Investigation that produces an auditable result.
_Avoid_: Arbitrary tool call, command

**Usage Event**:
An immutable, idempotent record of a billable unit produced by the service from a trusted source.
_Avoid_: Client-reported usage, counter update

**Billing Quote**:
A non-binding price calculation from trusted Usage Events and a plan; it is not a subscription or invoice.
_Avoid_: Bill, charge
