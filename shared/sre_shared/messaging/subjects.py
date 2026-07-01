"""
Canonical NATS JetStream subjects.

Naming conventions:
    sre.<domain>.<event>

Examples:
    sre.incidents.detected
    sre.diagnosis.complete
    sre.approval.requested
"""


# ===========================================================
# INCIDENTS stream
# ===========================================================
INCIDENT_DETECTED   = "sre.incidents.detected"
INCIDENT_RESOLVED   = "sre.incidents.resolved"
INCIDENT_ESCALATED  = "sre.incidents.escalated"
INCIDENT_STATE_CHANGED = "sre.incidents.state_changed"

# ===========================================================
#OPS stream
# ===========================================================
DIAGNOSIS_COMPLETE  = "sre.diagnosis.complete"
REMEDIATION_READY   = "sre.remediation.ready"
VERIFICATION_PASSED = "sre.verification.passed"
VERIFICATION_FAILED = "sre.verification.failed"
APPROVAL_REQUESTED  = "sre.approval.requested"
APPROVAL_RECEIVED   = "sre.approval.received"
EXECUTION_COMPLETE  = "sre.execution.complete"

# ===========================================================
#LEARNING stream
# ===========================================================
LEARNING_QUERY      = "sre.learning.query"
LEARNING_RESPONSE   = "sre.learning.response"

# ===========================================================
#AGENTS stream
# ===========================================================
AGENT_HEARTBEAT     = "sre.agent.heartbeat"