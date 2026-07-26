"""
Canonical NATS JetStream subjects.

Naming conventions:
    sre.<domain>.<event>

Examples:
    sre.incidents.detected
    sre.diagnosis.complete
    sre.approval.requested
"""


# =========================================================== #
# INCIDENTS stream                                            #
# =========================================================== #
INCIDENT_DETECTED   = "sre.incidents.detected"
INCIDENT_RESOLVED   = "sre.incidents.resolved"
INCIDENT_ESCALATED  = "sre.incidents.escalated"
INCIDENT_STATE_CHANGED = "sre.incidents.state_changed"
# state_changed: for state-centric consumers only (SLA tracking, audit
# analytics). Dashboard subscribes to OPS domain events directly, not this.


# =========================================================== #
#OPS stream                                                   #
# =========================================================== #

# Commands — Orchestrator dispatches the next agent per FSM transition
DIAGNOSIS_REQUESTED      = "sre.diagnosis.requested"
REMEDIATION_REQUESTED    = "sre.remediation.requested"
# payload may include prior_attempt_failure: {reason, failed_check, guidance}
# when this is a retry following a failed verification/safety review/rejection

SAFETY_REVIEW_REQUESTED  = "sre.safety.review_requested"
EXECUTION_REQUESTED      = "sre.execution.requested"

# Results — agent reports back to Orchestrator
DIAGNOSIS_COMPLETE  = "sre.diagnosis.complete"
REMEDIATION_READY   = "sre.remediation.ready"
VERIFICATION_PASSED = "sre.verification.passed"
VERIFICATION_FAILED = "sre.verification.failed"
SAFETY_REVIEW_PASSED   = "sre.safety.review_passed"
SAFETY_REVIEW_FAILED   = "sre.safety.review_failed"
EXECUTION_COMPLETE  = "sre.execution.complete"
# Internal to Safety <-> dashboard/human.
APPROVAL_REQUESTED  = "sre.approval.requested"
APPROVAL_RECEIVED   = "sre.approval.received"


# =========================================================== #
#LEARNING stream                                              #
# =========================================================== #
LEARNING_QUERY      = "sre.learning.query"

# =========================================================== #
#AGENTS stream                                                #
# =========================================================== #
AGENT_HEARTBEAT     = "sre.agent.heartbeat"

# ---------------------------------------------------------------------------
# DEFERRED — flagged for Phase 2 when runbook_engine.py is actually written:
# Remediator may need to propose multiple ranked candidate remediations
# (not just one) so Orchestrator can retry the next candidate on a failed
# verification/safety review without re-invoking Diagnoser. Revisit
# REMEDIATION_READY's payload shape (single action vs. ranked list) then.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Future streams — uncomment when a second consumer needs raw data
# ---------------------------------------------------------------------------
# OBSERVABILITY_METRIC    = "sre.observability.metric"
# OBSERVABILITY_LOG       = "sre.observability.log"
# OBSERVABILITY_ALERT     = "sre.observability.alert"
# OBSERVABILITY_TRACE     = "sre.observability.trace"
# PREDICTION_WARNING      = "sre.prediction.warning"
# NOTIFICATION_REQUESTED  = "sre.notification.requested"
# CLUSTER_REGISTERED      = "sre.clusters.registered"
# POLICY_UPDATED          = "sre.policy.updated"
# FEEDBACK_RUNBOOK        = "sre.feedback.runbook.submitted"