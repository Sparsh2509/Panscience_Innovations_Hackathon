"""
Configuration constants for NEXUS platform.
"""

# Default duration (in seconds) for time-bounded worker leases
DEFAULT_LEASE_DURATION_SECONDS: float = 10.0

# Default maximum retry attempts for failed jobs
DEFAULT_MAX_RETRIES: int = 3

# Default priority for jobs (higher numbers indicate higher priority)
DEFAULT_JOB_PRIORITY: int = 0

# Heartbeat interval: worker must heartbeat faster than lease duration
DEFAULT_HEARTBEAT_INTERVAL_SECONDS: float = 2.0

# Exponential backoff retry configuration
DEFAULT_RETRY_BASE_DELAY_SECONDS: float = 2.0
DEFAULT_RETRY_BACKOFF_FACTOR: float = 2.0
DEFAULT_RETRY_MAX_DELAY_SECONDS: float = 60.0

# Reaper scan interval
DEFAULT_REAPER_INTERVAL_SECONDS: float = 2.0

# Worker poll interval when queue is idle
DEFAULT_WORKER_POLL_INTERVAL_SECONDS: float = 0.5
