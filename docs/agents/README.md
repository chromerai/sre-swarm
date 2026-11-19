# Agent reference

The system is planned around six agents:

- Observer
- Diagnoser
- Remediator
- Safety
- Orchestrator
- Learner

The intended flow is hub-and-spoke through the Orchestrator:

```
Observer ──INCIDENT_DETECTED──▶ Orchestrator
                                  ├─ DIAGNOSIS_REQUESTED ──▶ Diagnoser
                                  │      ◀─ DIAGNOSIS_COMPLETE
                                  ├─ REMEDIATION_REQUESTED ──▶ Remediator (plan + verify)
                                  │      ◀─ REMEDIATION_READY / VERIFICATION_PASSED
                                  ├─ SAFETY_REVIEW_REQUESTED ──▶ Safety
                                  │      ◀─ SAFETY_REVIEW_PASSED
                                  ├─ EXECUTION_REQUESTED ──▶ Remediator (execute)
                                  │      ◀─ EXECUTION_COMPLETE
                                  └─ resolved ──INCIDENT_RESOLVED──▶ Learner
```

Agents do not hand off directly to one another. The Orchestrator dispatches
each command and receives every result. Diagnoser also queries Learner during
diagnosis, and Learner consumes resolved incidents asynchronously.

None of the agents are implemented yet. Each agent package currently contains
only its `src/` package skeleton.

Per-agent pages will be added to this directory as each agent is implemented.
