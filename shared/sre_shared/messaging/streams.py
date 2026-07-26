from nats.js.api import StreamConfig, RetentionPolicy, StorageType

NINETY_DAYS = 90 * 24 * 60 * 60
FIVE_MINS = 5 * 60
TWO_MINS = 2 * 60

INCIDENTS_STREAM = StreamConfig(
    name="INCIDENTS",
    subjects=["sre.incidents.*"],
    max_age=NINETY_DAYS,
    retention=RetentionPolicy.LIMITS,
    storage=StorageType.FILE,
    num_replicas=1,
    duplicate_window=TWO_MINS
)

OPS_STREAM = StreamConfig(
    name="OPS",
    subjects=[
        "sre.diagnosis.*", "sre.remediation.*",
        "sre.verification.*", "sre.approval.*", 
        "sre.execution.*", "sre.safety.*"
    ],
    max_age=NINETY_DAYS,
    retention=RetentionPolicy.LIMITS,
    storage=StorageType.FILE,
    num_replicas=1,
    duplicate_window=TWO_MINS
)

LEARNING_STREAM = StreamConfig(
    name="LEARNING",
    subjects=["sre.learning.*"],
    max_age=FIVE_MINS,
    retention=RetentionPolicy.LIMITS,
    storage=StorageType.MEMORY,
    num_replicas=1,
)

AGENTS_STREAM = StreamConfig(
    name="AGENTS",
    subjects=["sre.agent.*"],
    max_age=FIVE_MINS,
    retention=RetentionPolicy.LIMITS,
    storage=StorageType.MEMORY,
    num_replicas=1,
)



STREAM_CONFIGS = [
    INCIDENTS_STREAM,
    OPS_STREAM,
    LEARNING_STREAM,
    AGENTS_STREAM,
]