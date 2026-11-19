# Agent reference

The system is planned around six agents:

- Observer
- Diagnoser
- Remediator
- Safety
- Orchestrator
- Learner

The intended flow is:

```
Observer -> Diagnoser -> Remediator -> Safety -> Orchestrator
                                                       |
                                                    Learner
```

None of the agents are implemented yet. Each agent package currently contains
only its `src/` package skeleton.

Per-agent pages will be added to this directory as each agent is implemented.
